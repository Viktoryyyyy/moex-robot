import copy
import unittest
from moex_data.rub_production_source_matrix import build,unanalyzed_news


class ProductionSourceMatrixTests(unittest.TestCase):
    def snapshot(self):
        return dict(identity={'generated_at_utc':'2026-09-06T18:00:00+00:00'},components={
            'official_news':{'status':'READY','data':{'events':[dict(event_id='one',direction='NEUTRAL',confidence=0.0,rub_relevance=0.0,importance='LOW',horizon='SHORT_TERM',source_reference='https://example.org/news',published_at='2026-09-04T10:00:00+03:00')]}}})

    def test_news_placeholder_is_unknown_and_preserves_provenance(self):
        snapshot=self.snapshot();original=copy.deepcopy(snapshot)
        event=build(snapshot)['news_view'][0]
        self.assertEqual(event['direction'],'UNKNOWN');self.assertIsNone(event['confidence']);self.assertIsNone(event['rub_relevance'])
        self.assertEqual(event['source_reference'],'https://example.org/news')
        self.assertEqual(event['upstream_placeholder']['direction'],'NEUTRAL')
        self.assertEqual(snapshot,original)

    def test_projection_is_idempotent(self):
        events=self.snapshot()['components']['official_news']['data']['events']
        once=unanalyzed_news(events)
        self.assertEqual(once,unanalyzed_news(once))

    def test_collector_ready_does_not_grant_forecast_readiness(self):
        result=build(self.snapshot());news=next(r for r in result['rows'] if r['block_id']=='official_news')
        self.assertTrue(news['collection_present']);self.assertFalse(news['usable_for_full_forecast'])
        self.assertIn('official_news',result['blocking_required_blocks'])
        self.assertFalse(result['training_authorized'])

    def test_stale_market_does_not_get_closed_session_upgrade(self):
        snapshot=self.snapshot();snapshot['components']['synchronized_live_market_oi']={'status':'READY','data':{'instruments':{'si_front':{'last':80000,'price_oi_usable':False,'stale':True,'read_freshness_reason':'source_age_exceeds_threshold'}}}}
        result=build(snapshot);item=next(r for r in result['rows'] if r['block_id']=='si_front')
        self.assertTrue(item['collection_present']);self.assertFalse(item['usable_for_full_forecast'])

    def test_futoi_requires_explicit_consumer_authority(self):
        snapshot=self.snapshot();snapshot['components']['futoi_live']={'status':'READY','data':{'current_intraday':{'factual':{}},'consumer_factual_use_allowed':False,'factual_authority':True}}
        self.assertIn('futoi_live',build(snapshot)['blocking_required_blocks'])

    def test_volume_exclusion_never_hides_oil_or_fx(self):
        result=build(self.snapshot())
        self.assertNotIn('volume_features',result['blocking_required_blocks'])
        self.assertIn('brent',result['blocking_required_blocks']);self.assertIn('external_cny',result['blocking_required_blocks'])
        self.assertFalse(result['volume_investigation_in_scope'])


if __name__=='__main__':unittest.main()
