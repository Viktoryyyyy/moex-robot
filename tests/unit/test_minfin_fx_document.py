from datetime import datetime, timezone
import pytest

from moex_research.external_data import minfin_fx_document as m

NOW = datetime(2026, 9, 8, tzinfo=timezone.utc)
URL = 'https://minfin.gov.ru/ru/press-center/?id_4=40569-o_neftegazovykh_dokhodakh_i_provedenii_operatsii_po_pokupkeprodazhe_inostrannoi_valyuty_i_zolota_na_vnutrennem_valyutnom_rynke'


def fixture():
    return (f'<div data-post="minfin/9132"><div class="tgme_widget_message_text">'
        'Таким образом, совокупный объем средств, направляемых на покупку иностранной валюты и золота, составит 55,6 млрд руб. '
        'Операции будут проводиться в период с 7 сентября 2026 года по 6 октября 2026 года, соответственно, '
        'ежедневный объем покупки иностранной валюты и золота составит в эквиваленте 2,5 млрд руб.'
        f'<br><a href="{URL}">Подробнее</a></div><time datetime="2026-09-03T09:15:12+00:00"></time></div>').encode()


def test_exact_document_has_no_latest_or_authority():
    result = m.parse(fixture(), post_id=9132, received_at=NOW)
    assert result['direction'] == 'BUY'
    assert result['total_amount'] == '55.6'
    assert result['daily_amount'] == '2.5'
    assert result['operation_end'] == '2026-10-06'
    assert result['channel_published_at_utc'] == '2026-09-03T09:15:12+00:00'
    assert result['official_site_published_at'] is None
    assert not any(result[k] for k in ('latest_selection_proven', 'factual_authority', 'consumer_factual_use_allowed', 'historical_pit_acceptance', 'action_authority'))


@pytest.mark.parametrize('old,new', [
    ('minfin/9132', 'other/9132'), ('2026-09-03T09:15:12+00:00', '2026-09-09T09:15:12+00:00'),
    ('2026-09-03T09:15:12+00:00', '2026-09-03T09:15:12'), ('minfin.gov.ru', 'evil.example'),
    ('ежедневный объем покупки', 'ежедневный объем продажи'), ('2,5 млрд', '0 млрд'),
    ('2,5 млрд', '99 млрд'), ('6 октября', '6 августа'), ('6 октября', '31 февраля'),
])
def test_rejects_invalid_document(old, new):
    with pytest.raises(ValueError):
        m.parse(fixture().decode().replace(old, new).encode(), post_id=9132, received_at=NOW)


def test_duplicate_target_rejected():
    with pytest.raises(ValueError):
        m.parse(fixture() * 2, post_id=9132, received_at=NOW)


def test_forwarded_post_rejected():
    raw = fixture().replace(b'<div class="tgme_widget_message_text">', b'<a class="tgme_widget_message_forwarded_from">other</a><div class="tgme_widget_message_text">')
    with pytest.raises(ValueError):
        m.parse(raw, post_id=9132, received_at=NOW)


def test_neighbor_not_used_as_requested_post():
    neighbor = fixture().replace(b'minfin/9132', b'minfin/9133')
    assert m.parse(neighbor + fixture(), post_id=9132, received_at=NOW)['post_id'] == 9132
    with pytest.raises(ValueError):
        m.parse(neighbor, post_id=9132, received_at=NOW)


@pytest.mark.parametrize('post_id', [True, 0, -1, '9132', 100000000])
def test_post_id_bounds(post_id):
    with pytest.raises(ValueError):
        m.post_url(post_id)


def test_linkage_requires_exact_anchor():
    url, target = next(iter(m.LINKAGE.items()))
    m.verify_linkage(f'<a href="{target}">Источник</a>'.encode(), url)
    with pytest.raises(ValueError):
        m.verify_linkage(target.encode(), url)
    with pytest.raises(ValueError):
        m.verify_linkage(b'<a href="https://t.me/other/6717">source</a>', url)


@pytest.mark.parametrize('url', ['http://t.me/s/minfin/9132', 'https://t.me/s/other/9132', 'https://t.me/s/minfin/9132?x=1', 'https://t.me/s/minfin/9132/../x'])
def test_fetch_rejects_route_before_network(url):
    with pytest.raises(ValueError):
        m.fetch(url)


def test_capture_archives_receipts_without_authority(tmp_path, monkeypatch):
    def fetch(url):
        raw = fixture() if url == m.post_url(9132) else f'<a href="{m.LINKAGE[url]}">source</a>'.encode()
        return raw, {'source_url': url, 'requested_at_utc': NOW.isoformat(), 'received_at_utc': NOW.isoformat(), 'raw_sha256': m.sha256(raw).hexdigest()}
    monkeypatch.setattr(m, 'fetch', fetch)
    result = m.capture(post_id=9132, output=tmp_path)
    assert result['channel_linkage_status'] == 'GOVERNMENT_SOURCE_REFERENCES_VERIFIED'
    assert len(result['linkage_receipts']) == 2
    assert len(list(tmp_path.glob('*.html'))) == 3
    assert len(list(tmp_path.glob('*.json'))) == 1
    assert result['factual_authority'] is False


def test_capture_rejects_missing_linkage(tmp_path, monkeypatch):
    monkeypatch.setattr(m, 'fetch', lambda url: (b'<html>no source</html>', {}))
    with pytest.raises(ValueError):
        m.capture(post_id=9132, output=tmp_path)
