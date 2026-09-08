"""Bounded acquisition of one Minfin channel FX announcement; no latest/API admission."""
import argparse
from datetime import date, datetime, timezone
from decimal import Decimal
from hashlib import sha256
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import ssl
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener

MAX_BYTES = 2_000_000
LINKAGE = {
    'https://mintrans.gov.ru/press-center/branch-news/6845': 'https://t.me/minfin/6717',
    'https://mintrans.gov.ru/press-center/branch-news/8257': 'https://t.me/minfin/7910',
}
MONTHS = 'января февраля марта апреля мая июня июля августа сентября октября ноября декабря'.split()


def post_url(post_id):
    if isinstance(post_id, bool) or not isinstance(post_id, int) or not 1 <= post_id <= 99999999:
        raise ValueError('invalid exact post id')
    return f'https://t.me/s/minfin/{post_id}'


class _HTML(HTMLParser):
    def __init__(self, post_id=None):
        super().__init__()
        self.target = f'minfin/{post_id}'
        self.stack = []
        self.posts = 0
        self.text = []
        self.times = []
        self.links = []
        self.all_links = []
        self.forwarded = False

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        active = bool(self.stack and self.stack[-1][1])
        in_text = bool(self.stack and self.stack[-1][2])
        if attrs.get('data-post') == self.target:
            self.posts += 1
            active = True
        if active and 'tgme_widget_message_text' in attrs.get('class', '').split():
            in_text = True
        if active and 'tgme_widget_message_forwarded_from' in attrs.get('class', '').split():
            self.forwarded = True
        if tag == 'a':
            self.all_links.append(attrs.get('href'))
            if active:
                self.links.append(attrs.get('href'))
        if tag == 'time' and active:
            self.times.append(attrs.get('datetime'))
        if tag == 'br' and in_text:
            self.text.append(' ')
        if tag not in {'area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input', 'link', 'meta', 'param', 'source', 'track', 'wbr'}:
            self.stack.append((tag, active, in_text))

    def handle_endtag(self, tag):
        if self.stack and self.stack[-1][0] == tag:
            self.stack.pop()

    def handle_data(self, data):
        if self.stack and self.stack[-1][2]:
            self.text.append(data)


def _html(raw, post_id=None):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_BYTES:
        raise ValueError('invalid HTML size')
    parser = _HTML(post_id)
    parser.feed(raw.decode('utf-8'))
    return parser


def verify_linkage(raw, url):
    if url not in LINKAGE or LINKAGE[url] not in _html(raw).all_links:
        raise ValueError('government channel linkage absent')


def _one(pattern, text):
    matches = re.findall(pattern, text)
    if len(matches) != 1:
        raise ValueError('ambiguous or unsupported announcement format')
    return matches[0]


def parse(raw, *, post_id, received_at):
    post_url(post_id)
    if not isinstance(received_at, datetime) or received_at.utcoffset() is None:
        raise ValueError('aware receipt required')
    parsed = _html(raw, post_id)
    if parsed.posts != 1 or len(parsed.times) != 1 or not isinstance(parsed.times[0], str) or parsed.forwarded:
        raise ValueError('exact post or timestamp absent/ambiguous')
    published = datetime.fromisoformat(parsed.times[0])
    if published.utcoffset() is None or published > received_at:
        raise ValueError('invalid channel publication timestamp')
    official = [x for x in parsed.links if x and re.fullmatch(
        r'https://minfin\.gov\.ru/ru/press-center/\?id_4=\d+-o_neftegazovykh_dokhodakh_i_provedenii_operatsii_po_pokupkeprodazhe_inostrannoi_valyuty_i_zolota_na_vnutrennem_valyutnom_rynke', x)]
    if len(official) != 1:
        raise ValueError('official announcement reference absent/ambiguous')
    text = ' '.join(''.join(parsed.text).split())
    direction, amount = _one(r'совокупный объем средств, направляемых на (покупку|продажу) иностранной валюты и золота, составит (\d+(?:,\d+)?) млрд руб\.', text)
    daily_direction, daily = _one(r'ежедневный объем (покупки|продажи) иностранной валюты и золота составит в эквиваленте (\d+(?:,\d+)?) млрд руб\.', text)
    if (direction == 'покупку') != (daily_direction == 'покупки'):
        raise ValueError('operation direction conflict')
    parts = _one(r'Операции будут проводиться в период с (\d{1,2}) ([а-я]+) (\d{4}) года по (\d{1,2}) ([а-я]+) (\d{4}) года', text)
    start = date(int(parts[2]), MONTHS.index(parts[1]) + 1, int(parts[0]))
    end = date(int(parts[5]), MONTHS.index(parts[4]) + 1, int(parts[3]))
    total, per_day = Decimal(amount.replace(',', '.')), Decimal(daily.replace(',', '.'))
    if start > end or (end - start).days > 62 or not 0 < per_day <= total:
        raise ValueError('invalid operation period or amounts')
    return {'schema_version': 'minfin_fx_document.v1', 'post_id': post_id,
        'source_url': post_url(post_id), 'official_announcement_url': official[0],
        'channel_published_at_utc': published.astimezone(timezone.utc).isoformat(),
        'official_site_published_at': None, 'received_at_utc': received_at.astimezone(timezone.utc).isoformat(),
        'raw_sha256': sha256(raw).hexdigest(), 'operation_start': start.isoformat(), 'operation_end': end.isoformat(),
        'direction': 'BUY' if direction == 'покупку' else 'SELL', 'asset_scope': 'FX_AND_GOLD',
        'amount_unit': 'RUB_BILLION', 'total_amount': str(total), 'daily_amount': str(per_day),
        'operation_scope': 'ANNOUNCED_PLAN_NOT_EXECUTION', 'selection_scope': 'EXACT_CALLER_SELECTED_POST',
        'latest_selection_proven': False, 'factual_authority': False, 'consumer_factual_use_allowed': False,
        'historical_pit_acceptance': False, 'action_authority': False}


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError('source redirect refused')


def fetch(url, *, timeout=15):
    if url not in LINKAGE and not re.fullmatch(r'https://t\.me/s/minfin/[1-9]\d{0,7}', url):
        raise ValueError('URL outside bounded source scope')
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout <= 30:
        raise ValueError('invalid timeout')
    opener = build_opener(_NoRedirect(), HTTPSHandler(context=ssl.create_default_context()))
    requested = datetime.now(timezone.utc)
    with opener.open(Request(url, headers={'User-Agent': 'moex-robot-minfin-document/1', 'Accept': 'text/html'}), timeout=timeout) as response:
        if response.status != 200 or response.geturl() != url or response.headers.get_content_type() != 'text/html':
            raise ValueError('unexpected source response')
        raw = response.read(MAX_BYTES + 1)
    received = datetime.now(timezone.utc)
    if not 0 < len(raw) <= MAX_BYTES or received < requested:
        raise ValueError('invalid receipt or size')
    return raw, {'source_url': url, 'requested_at_utc': requested.isoformat(),
        'received_at_utc': received.isoformat(), 'raw_sha256': sha256(raw).hexdigest()}


def _freeze(directory, raw, suffix):
    digest = sha256(raw).hexdigest()
    path = directory / (digest + suffix)
    if path.is_symlink():
        raise ValueError('evidence symlink refused')
    try:
        with path.open('xb') as stream:
            stream.write(raw)
    except FileExistsError:
        if path.read_bytes() != raw:
            raise ValueError('evidence collision')
    return digest


def capture(*, post_id, output):
    url = post_url(post_id)
    directory = Path(output)
    if any(p.is_symlink() for p in (directory, *directory.parents)):
        raise ValueError('evidence directory symlink refused')
    directory.mkdir(parents=True, exist_ok=True)
    evidence = []
    for link in LINKAGE:
        raw, receipt = fetch(link)
        verify_linkage(raw, link)
        _freeze(directory, raw, '.html')
        evidence.append(receipt)
    raw, receipt = fetch(url)
    _freeze(directory, raw, '.html')
    result = parse(raw, post_id=post_id, received_at=datetime.fromisoformat(receipt['received_at_utc']))
    result.update(channel_linkage_status='GOVERNMENT_SOURCE_REFERENCES_VERIFIED', linkage_receipts=evidence, post_receipt=receipt)
    encoded = json.dumps(result, sort_keys=True, separators=(',', ':')).encode()
    digest = _freeze(directory, encoded, '.json')
    return {**result, 'manifest_sha256': digest, 'manifest_path': str(directory / (digest + '.json'))}


if __name__ == '__main__':
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--post-id', required=True, type=int)
    cli.add_argument('--output', required=True, type=Path)
    args = cli.parse_args()
    print(json.dumps(capture(post_id=args.post_id, output=args.output), indent=2))
