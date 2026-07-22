from __future__ import annotations
import argparse, math
from dataclasses import asdict
from datetime import date
import pandas as pd
from moex_research.external_data.moex_cnyrub_algopack_history import *
from moex_research.external_data.moex_cnyrub_history import CnyrubHistoryError
from moex_research.runners import usdrubf_phase8_6a_moex_cnyrub_source_validation as base

PROJECT="MOEX Bot"; PHASE="8.6A"; LANE="ema_3_19_ai"
TASK_ID="ema_3_19_ai_phase_8_6a_algopack_cnyrub_source_validation_v2"
EXECUTION_MODE="browser_chatgpt_github_direct"
CONTRACT_ID="usdrubf_phase8_6a_algopack_cnyrub_source_validation_v2"; CONTRACT_VERSION="2.0"
APPROVED_BRANCH="research/ema-3-19-ai/phase-8-6a-algopack-cnyrub-source-validation-v2"
EXPECTED_INPUT_SHA256=base.EXPECTED_INPUT_SHA256; EXPECTED_ELIGIBLE_IDENTITIES=base.EXPECTED_ELIGIBLE_IDENTITIES
EXPECTED_VALIDATION_IDENTITIES=base.EXPECTED_VALIDATION_IDENTITIES; EXPECTED_FOLDS=base.EXPECTED_FOLDS
EXPECTED_VALIDATION_ROWS_PER_FOLD=base.EXPECTED_VALIDATION_ROWS_PER_FOLD; IDENTITY_COLUMNS=base.IDENTITY_COLUMNS
DECLARED_OUTPUT_ARTIFACTS=base.DECLARED_OUTPUT_ARTIFACTS; REQUIRED_CLI_ARGS=base.REQUIRED_CLI_ARGS
Phase86ARequest=base.Phase86ARequest; Phase86AResult=base.Phase86AResult; _FORBIDDEN_MATRIX_FIELDS=base._FORBIDDEN_MATRIX_FIELDS; _SHA256=base._SHA256
DIAGNOSTIC_COLUMNS=("target_trade_date","prior_trade_date","candidate_trade_date","accepted","reason","blocker_classification","same_day_or_future_used","forward_fill_used","backward_fill_used","arbitrary_date_selection_used","source_substitution_used")
ACCEPTANCE_MATRIX_COLUMNS=("target_trade_date","target_instrument_id","prior_trade_date","cnyrub_security_id","cnyrub_board_id","cnyrub_trade_date","cnyrub_open","cnyrub_high","cnyrub_low","cnyrub_close","cnyrub_volume","cnyrub_volume_buy","cnyrub_volume_sell","cnyrub_volume_imbalance","cnyrub_value","cnyrub_value_buy","cnyrub_value_sell","cnyrub_trades","cnyrub_trades_buy","cnyrub_trades_sell","cnyrub_candle_begin","cnyrub_candle_end","cnyrub_source_available_at","cnyrub_source_route","cnyrub_payload_sha256","cnyrub_retrieved_at_utc","cnyrub_source_revision_status")
NORMALIZED_SOURCE_COLUMNS=("source_id","security_id","board_id","engine","market","trade_date","open","high","low","close","volume","volume_buy","volume_sell","volume_imbalance","value","value_buy","value_sell","trades","trades_buy","trades_sell","candle_begin","candle_end","source_available_at","source_route","retrieved_at_utc","raw_payload_sha256","source_revision_status","historical_model_use_status")
BLOCKER_CLASSIFICATIONS=("security_identity_not_reproducible","token_env_not_configured","algopack_authentication_failed","algopack_subscription_not_entitled","official_route_not_reproducible","cnyrub_tom_not_available","algopack_rate_limit_blocked","algopack_tradestats_not_available","algopack_schema_not_stable","official_schema_not_stable","point_in_time_cutoff_not_provable","incomplete_identity_coverage","numerical_or_chronology_integrity_failure","provenance_not_sufficient","other_fail_closed_with_exact_reason")
EXPECTED_TRANSIENT_HTTP_RETRY_POLICY={"bounded_transient_retry_enabled":True,"enabled_for_source_id":SOURCE_ID,"phase_scope":"8.6A_algopack_v2_only","maximum_total_attempts":ALGOPACK_HTTP_MAX_ATTEMPTS,"retry_delays_seconds":list(ALGOPACK_HTTP_RETRY_DELAYS_SECONDS),"maximum_retry_after_seconds":ALGOPACK_MAX_RETRY_AFTER_SECONDS,"random_jitter_allowed":False,"same_exact_official_route_only":True,"redirects_allowed":False,"cross_host_redirect_allowed":False,"route_substitution_allowed":False,"fallback_source_allowed":False,"fallback_security_allowed":False,"fallback_board_allowed":False,"fallback_date_allowed":False,"retryable_outcomes":["HTTP_429","HTTP_5XX","transport_timeout"],"non_retryable_outcomes":["HTTP_401","HTTP_403","HTTP_404","schema_failure"],"semantic_failures_retried":False}

class Phase86AAlgoPackSourceValidationError(ValueError):
 def __init__(self,message,*,blocker="other_fail_closed_with_exact_reason"): super().__init__(message); self.blocker=blocker if blocker in BLOCKER_CLASSIFICATIONS else "other_fail_closed_with_exact_reason"

def build_argument_parser():
 p=argparse.ArgumentParser(prog="python -m moex_research.runners.usdrubf_phase8_6a_algopack_cnyrub_source_validation")
 for f in REQUIRED_CLI_ARGS:p.add_argument(f,required=True)
 return p
def request_from_args(a):return base.request_from_args(a)
def build_metadata_route():return build_security_metadata_url()

def _validate_experiment_contract(c):
 ident={"contract_id":CONTRACT_ID,"contract_version":CONTRACT_VERSION,"project":PROJECT,"task_id":TASK_ID,"lane":LANE,"phase":PHASE,"execution_mode":EXECUTION_MODE,"status":"source_validation_only"}; s=c.get("source_identity",{})
 checks=(c.get("contract_identity")==ident,c.get("approved_branch")==APPROVED_BRANCH,tuple(c.get("runtime_artifacts",()))==DECLARED_OUTPUT_ARTIFACTS,c.get("transient_http_retry_policy")==EXPECTED_TRANSIENT_HTTP_RETRY_POLICY,tuple(c.get("acceptance_matrix_fields",()))==ACCEPTANCE_MATRIX_COLUMNS,tuple(c.get("normalized_source_required_fields",()))==NORMALIZED_SOURCE_COLUMNS,c.get("required_environment_variables")==[ALGOPACK_TOKEN_ENV],tuple(c.get("blocker_classifications",()))==BLOCKER_CLASSIFICATIONS)
 if not all(checks):raise Phase86AAlgoPackSourceValidationError("Phase 8.6A AlgoPack contract mismatch")
 if tuple(s.get(k) for k in ("security_id","board_id","engine","market"))!=(SECURITY_ID,BOARD_ID,ENGINE,MARKET):raise Phase86AAlgoPackSourceValidationError("Phase 8.6A AlgoPack source identity mismatch")
 if (s.get("source_id"),s.get("official_service"),s.get("tradestats_route"))!=(SOURCE_ID,"MOEX AlgoPack subscription",ALGOPACK_TRADESTATS_ROUTE):raise Phase86AAlgoPackSourceValidationError("Phase 8.6A AlgoPack service or route mismatch")

def normalized_candles(c):return pd.DataFrame([x.as_record() for x in c],columns=NORMALIZED_SOURCE_COLUMNS)
def _empty_source_row():return {c:None for c in ACCEPTANCE_MATRIX_COLUMNS[3:]}

def build_cnyrub_pit_acceptance_matrix(eligible,candles):
 keyed={}
 for c in candles:
  if c.trade_date in keyed:raise Phase86AAlgoPackSourceValidationError("duplicate AlgoPack CNYRUB date",blocker="numerical_or_chronology_integrity_failure")
  keyed[c.trade_date]=c
 rows=[]; diags=[]
 for i in eligible.itertuples(index=False):
  target=date.fromisoformat(i.target_trade_date); prior=date.fromisoformat(i.prior_trade_date); c=keyed.get(prior); accepted=False; blocker=None; candidate=None if c is None else c.trade_date.isoformat()
  if c is None: src=_empty_source_row(); reason="missing_exact_prior_trade_date_algopack_aggregate"
  else:
   try:validate_prior_session_candle(c,target_trade_date=target,prior_trade_date=prior)
   except CnyrubAlgoPackError as e:
    if e.blocker!="point_in_time_cutoff_not_provable":raise Phase86AAlgoPackSourceValidationError(str(e),blocker=e.blocker) from e
    src=_empty_source_row(); reason=str(e); blocker=e.blocker
   else:
    accepted=True; reason="accepted_exact_prior_trade_date_algopack_aggregate"; src={"cnyrub_security_id":c.security_id,"cnyrub_board_id":c.board_id,"cnyrub_trade_date":c.trade_date.isoformat(),"cnyrub_open":c.open,"cnyrub_high":c.high,"cnyrub_low":c.low,"cnyrub_close":c.close,"cnyrub_volume":c.volume,"cnyrub_volume_buy":c.volume_buy,"cnyrub_volume_sell":c.volume_sell,"cnyrub_volume_imbalance":c.volume_imbalance,"cnyrub_value":c.value,"cnyrub_value_buy":c.value_buy,"cnyrub_value_sell":c.value_sell,"cnyrub_trades":c.trades,"cnyrub_trades_buy":c.trades_buy,"cnyrub_trades_sell":c.trades_sell,"cnyrub_candle_begin":c.candle_begin.isoformat(),"cnyrub_candle_end":c.candle_end.isoformat(),"cnyrub_source_available_at":c.source_available_at.isoformat(),"cnyrub_source_route":c.source_route,"cnyrub_payload_sha256":c.raw_payload_sha256,"cnyrub_retrieved_at_utc":c.retrieved_at_utc.isoformat().replace("+00:00","Z"),"cnyrub_source_revision_status":c.source_revision_status}
  rows.append({"target_trade_date":i.target_trade_date,"target_instrument_id":i.target_instrument_id,"prior_trade_date":i.prior_trade_date,**src}); diags.append({"target_trade_date":i.target_trade_date,"prior_trade_date":i.prior_trade_date,"candidate_trade_date":candidate,"accepted":accepted,"reason":reason,"blocker_classification":blocker,"same_day_or_future_used":False,"forward_fill_used":False,"backward_fill_used":False,"arbitrary_date_selection_used":False,"source_substitution_used":False})
 return pd.DataFrame(rows,columns=ACCEPTANCE_MATRIX_COLUMNS),pd.DataFrame(diags,columns=DIAGNOSTIC_COLUMNS)

def _coverage(m,v):
 mask=pd.MultiIndex.from_frame(m.loc[:,IDENTITY_COLUMNS]).isin(pd.MultiIndex.from_frame(v)); complete=m.loc[:,ACCEPTANCE_MATRIX_COLUMNS[3:]].notna().all(axis=1); ec=int(complete.sum()); vc=int(mask.sum()); vv=int(complete.to_numpy()[mask].sum())
 return pd.DataFrame([{"source_id":SOURCE_ID,"eligible_identity_count":len(m),"eligible_covered_count":ec,"eligible_missing_count":len(m)-ec,"eligible_coverage_pct":ec/len(m)*100 if len(m) else 0.0,"validation_identity_count":vc,"validation_covered_count":vv,"validation_missing_count":vc-vv,"validation_coverage_pct":vv/vc*100 if vc else 0.0}])

def _identity_record(i):return ({**asdict(i),"identity_verified":True,"identity_service":"MOEX ISS metadata","data_source_id":SOURCE_ID,"data_service":"MOEX AlgoPack subscription"} if i else {"source_id":SOURCE_ID,"security_id":SECURITY_ID,"board_id":BOARD_ID,"engine":ENGINE,"market":MARKET,"identity_verified":False,"historical_model_use_status":"blocked"})
def _official_route_validation(i,c,error=None):return {"official_service":"MOEX AlgoPack subscription","official_host":ALGOPACK_HOST,"authorization_scheme":"Bearer","token_environment_variable":ALGOPACK_TOKEN_ENV,"token_persisted_in_artifacts":False,"authorization_header_persisted":False,"redirects_allowed":False,"security_metadata_route":build_metadata_route(),"tradestats_route":ALGOPACK_TRADESTATS_ROUTE,"bucket_interval_minutes":ALGOPACK_BUCKET_MINUTES,"provider_availability_field":"SYSTIME","verified_security_id":i.security_id if i else None,"verified_board_id":i.board_id if i else None,"verified_engine":i.engine if i else None,"verified_market":i.market if i else None,"primary_board":bool(i and i.primary_board),"active_board":bool(i and i.active_board),"pagination_complete":error is None,"schema_stable_within_run":error is None,"earliest_available_date":None if c.empty else str(c.trade_date.min()),"latest_available_date":None if c.empty else str(c.trade_date.max()),"daily_aggregate_count":len(c),"directional_volume_fields_present":bool(not c.empty and {"volume_buy","volume_sell"}.issubset(c.columns)),"source_availability_present":bool(not c.empty and c.source_available_at.notna().all()),"fallback_used":False,"source_revision_status":SOURCE_REVISION_STATUS}
def _finite(f,cols):return True if f.empty else bool(f.loc[:,cols].apply(pd.to_numeric,errors="coerce").map(math.isfinite).all().all())

def evaluate_gates(*,immutable_inputs_verified,phase83_verified,eligible,validation,identity,candles,matrix,coverage,diagnostics,route_validation,source_error=None):
 g1=bool(immutable_inputs_verified and phase83_verified and len(eligible)==EXPECTED_ELIGIBLE_IDENTITIES and len(validation)==EXPECTED_VALIDATION_IDENTITIES); g2=bool(identity and (identity.security_id,identity.board_id,identity.engine,identity.market)==(SECURITY_ID,BOARD_ID,ENGINE,MARKET) and identity.primary_board and identity.active_board)
 g3=bool(route_validation.get("official_host")==ALGOPACK_HOST and route_validation.get("tradestats_route")==ALGOPACK_TRADESTATS_ROUTE and route_validation.get("bucket_interval_minutes")==ALGOPACK_BUCKET_MINUTES and route_validation.get("pagination_complete") and route_validation.get("schema_stable_within_run") and route_validation.get("directional_volume_fields_present") and route_validation.get("source_availability_present") and route_validation.get("redirects_allowed") is False)
 a=matrix.cnyrub_trade_date.notna(); target=pd.to_datetime(matrix.loc[a,"target_trade_date"]); prior=pd.to_datetime(matrix.loc[a,"prior_trade_date"]); observed=pd.to_datetime(matrix.loc[a,"cnyrub_trade_date"]); available=pd.to_datetime(matrix.loc[a,"cnyrub_source_available_at"],utc=True).dt.tz_convert("Europe/Moscow"); nofill=not diagnostics[["same_day_or_future_used","forward_fill_used","backward_fill_used","arbitrary_date_selection_used","source_substitution_used"]].any().any(); pit=diagnostics.blocker_classification.eq("point_in_time_cutoff_not_provable").any()
 g4=bool(not(source_error and source_error.blocker=="point_in_time_cutoff_not_provable") and not pit and observed.eq(prior).all() and available.lt(target.dt.tz_localize("Europe/Moscow")+pd.Timedelta(hours=6)).all() and nofill)
 cr=coverage.iloc[0]; g5=bool(len(matrix)==EXPECTED_ELIGIBLE_IDENTITIES and int(cr.eligible_covered_count)==EXPECTED_ELIGIBLE_IDENTITIES and int(cr.validation_covered_count)==EXPECTED_VALIDATION_IDENTITIES and not matrix.duplicated(list(IDENTITY_COLUMNS)).any() and matrix.loc[:,IDENTITY_COLUMNS].equals(eligible.loc[:,IDENTITY_COLUMNS]) and tuple(matrix.columns)==ACCEPTANCE_MATRIX_COLUMNS)
 vi=bool(candles.empty or (candles.volume-(candles.volume_buy+candles.volume_sell)).abs().le(1e-9).all()); vai=bool(candles.empty or (candles.value-(candles.value_buy+candles.value_sell)).abs().le(candles.value.abs().mul(1e-6).clip(lower=1.0)).all()); ti=bool(candles.empty or candles.trades.eq(candles.trades_buy+candles.trades_sell).all()); nums=("open","high","low","close","volume","volume_buy","volume_sell","volume_imbalance","value","value_buy","value_sell","trades","trades_buy","trades_sell")
 g6=bool(_finite(candles,nums) and (candles.empty or (pd.to_datetime(candles.trade_date).is_monotonic_increasing and not candles.duplicated(["trade_date"]).any())) and (candles.empty or (candles.high.ge(candles[["open","close","low"]].max(axis=1)).all() and candles.low.le(candles[["open","close","high"]].min(axis=1)).all())) and vi and vai and ti and (candles.empty or candles.volume_imbalance.between(-1,1).all()))
 p=matrix.loc[a,["cnyrub_security_id","cnyrub_board_id","cnyrub_source_route","cnyrub_payload_sha256","cnyrub_retrieved_at_utc","cnyrub_source_revision_status"]]; rev=bool(matrix.cnyrub_source_revision_status.notna().all() and matrix.cnyrub_source_revision_status.eq(SOURCE_REVISION_STATUS).all()); g7=bool(p.notna().all().all() and p.cnyrub_security_id.eq(SECURITY_ID).all() and p.cnyrub_board_id.eq(BOARD_ID).all() and p.cnyrub_source_route.astype(str).str.startswith(ALGOPACK_TRADESTATS_ROUTE+"?").all() and p.cnyrub_payload_sha256.astype(str).map(lambda x:bool(_SHA256.fullmatch(x))).all() and rev); g8=bool(not set(matrix.columns)&_FORBIDDEN_MATRIX_FIELDS and not diagnostics.source_substitution_used.any() and not diagnostics.arbitrary_date_selection_used.any())
 passed=(g1,g2,g3,g4,g5,g6,g7,g8); names=("G1_immutable_inputs","G2_official_security_identity","G3_algopack_tradestats_route_and_schema","G4_point_in_time_session_correctness","G5_exact_coverage","G6_directional_volume_and_numerical_integrity","G7_provenance"); gates={n:{"passed":v} for n,v in zip(names,passed[:7],strict=True)}
 gates[names[3]].update({"pit_rejection_count":int(diagnostics.blocker_classification.eq("point_in_time_cutoff_not_provable").sum()),"provider_availability_field":"SYSTIME","source_available_before_anchor":g4,"fill_or_substitution_used":not nofill}); gates[names[5]].update({"volume_equals_buy_plus_sell":vi,"value_equals_buy_plus_sell":vai,"trades_equal_buy_plus_sell":ti}); gates[names[6]].update({"source_revision_status_required":SOURCE_REVISION_STATUS,"source_revision_status_valid":rev,"token_persisted_in_artifacts":False}); gates["G8_leakage_and_scope"]={"passed":g8,"model_file_created":False,"model_fit_or_evaluation_performed":False,"target_prediction_or_probability_used":False,"source_fallback_used":False,"out_of_directory_write_performed":False,"promotion_performed":False,"broker_or_trading_action_performed":False}
 failed=[f"G{i}" for i,v in enumerate(passed,1) if not v]; blocker=None if not failed else _blocker_from_failure(failed,source_error); gates["G9_final_source_readiness"]={"passed":not failed,"requires":[f"G{i}" for i in range(1,9)],"failed_gates":failed,"status":"moex_algopack_cnyrub_source_candidate_for_phase8_6b" if not failed else "moex_algopack_cnyrub_source_not_ready","historical_model_use_status":HISTORICAL_MODEL_USE_STATUS if not failed else "blocked","blocker_classification":blocker}; return gates

def _blocker_from_failure(failed,error):
 if error and error.blocker in BLOCKER_CLASSIFICATIONS:return error.blocker
 if "G4" in failed:return "point_in_time_cutoff_not_provable"
 return next((v for k,v in {"G2":"security_identity_not_reproducible","G3":"algopack_schema_not_stable","G5":"incomplete_identity_coverage","G6":"numerical_or_chronology_integrity_failure","G7":"provenance_not_sufficient"}.items() if k in failed),"other_fail_closed_with_exact_reason")
def _structured_reason(blocker,diagnostics,error):
 if blocker is None:return None
 x=diagnostics.loc[diagnostics.blocker_classification.eq(blocker),"reason"]
 return str(x.iloc[0]) if not x.empty else (str(error) if error is not None else blocker)

def run_source_validation(request,*,algopack_transport=fetch_algopack_bytes,token_loader=load_algopack_token,clock=utc_now,identity_loader=load_security_identity,history_loader=load_daily_history):
 base._validate_request(request); hashes=base.verify_immutable_inputs(request); aggregate=base._json(request.phase83_aggregate_metrics_path); phase83=base._json(request.phase83_gate_results_path); base._validate_phase83_evidence(aggregate,phase83); _validate_experiment_contract(base._json(request.experiment_contract_path)); base._json(request.dataset_manifest_path); base._json(request.feature_schema_path)
 eligible=base._eligible_identities(pd.read_parquet(request.modeling_dataset_path)); validation=base._validation_identities(pd.read_parquet(request.m0_validation_predictions_path),eligible); identity=None; error=None; candle_list=[]
 try:
  identity=identity_loader(clock=clock); first=min(map(date.fromisoformat,eligible.prior_trade_date)); last=max(map(date.fromisoformat,eligible.prior_trade_date)); candle_list=history_loader(identity,from_date=first,till_date=last,transport=algopack_transport,token_loader=token_loader,clock=clock)
 except (CnyrubAlgoPackError,CnyrubHistoryError) as e:
  if e.blocker not in BLOCKER_CLASSIFICATIONS:raise Phase86AAlgoPackSourceValidationError(str(e),blocker=e.blocker) from e
  error=e
 candles=normalized_candles(candle_list); matrix,diagnostics=build_cnyrub_pit_acceptance_matrix(eligible,candle_list); coverage=_coverage(matrix,validation); routes=_official_route_validation(identity,candles,error); gates=evaluate_gates(immutable_inputs_verified=True,phase83_verified=True,eligible=eligible,validation=validation,identity=identity,candles=candles,matrix=matrix,coverage=coverage,diagnostics=diagnostics,route_validation=routes,source_error=error); final=gates["G9_final_source_readiness"]
 inputs={"project":PROJECT,"phase":PHASE,"task_id":TASK_ID,"run_id":request.run_id,"source_git_commit_sha":request.git_commit_sha,"eligible_identity_count":len(eligible),"validation_identity_count":len(validation),"expected_folds":EXPECTED_FOLDS,"expected_validation_rows_per_fold":EXPECTED_VALIDATION_ROWS_PER_FOLD,"frozen_target_interval":[eligible.target_trade_date.iloc[0],eligible.target_trade_date.iloc[-1]],"immutable_inputs":{n:{"expected_sha256":e,"observed_sha256":hashes[n],"matches":hashes[n]==e} for n,e in EXPECTED_INPUT_SHA256.items()}}
 register={"source_id":SOURCE_ID,"status":final["status"],"historical_model_use_status":final["historical_model_use_status"],"blocker_classification":final["blocker_classification"],"exact_blocker_reason":_structured_reason(final["blocker_classification"],diagnostics,error),"failed_gates":final["failed_gates"],"offending_candle_used_in_acceptance_matrix":False,"fill_or_substitution_used":False,"source_fallback_used":False,"subscription_token_persisted":False,"authorization_header_persisted":False,"response_body_persisted_in_blocker":False,"model_fit_or_evaluation_performed":False,"promotion_authorized":False}
 payloads={"input_identity_verification.json":inputs,"official_route_validation.json":routes,"cnyrub_security_identity.json":_identity_record(identity),"cnyrub_daily_candles_normalized.parquet":candles,"cnyrub_pit_acceptance_matrix.parquet":matrix,"coverage_by_source.csv":coverage,"session_alignment_diagnostics.csv":diagnostics,"source_blocker_register.json":register,"gate_results.json":gates}; base._write_exact_artifacts(request.output_dir,payloads); return Phase86AResult(request.output_dir,DECLARED_OUTPUT_ARTIFACTS,len(eligible),len(validation),str(final["status"]),final["blocker_classification"])

def run_from_args(a):return run_source_validation(request_from_args(a))
def main(argv=None):run_from_args(build_argument_parser().parse_args(argv)); return 0
if __name__=="__main__":raise SystemExit(main())
