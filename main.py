
import pandas as pd
from pathlib import Path
from dateutil.relativedelta import relativedelta

def to_naive_datetime(series):
    s = pd.to_datetime(series, errors="coerce")
    try:
        s = s.dt.tz_localize(None)
    except Exception:
        pass
    return s

def build_star_schema(excel_path: Path, outdir: Path) -> None:
    xls = pd.ExcelFile(excel_path)
    dfs = {name: pd.read_excel(excel_path, sheet_name=name) for name in xls.sheet_names}
    plan = dfs.get("plan").copy()
    plan_freq = dfs.get("plan_payment_frequency").copy()
    status_code = dfs.get("status_code").copy()
    user = dfs.get("user").copy()
    user_payment_detail = dfs.get("user_payment_detail").copy()
    user_plan = dfs.get("user_plan").copy()
    user_play_session = dfs.get("user_play_session").copy()
    user_registration = dfs.get("user_registration").copy()

    # Dimensions
    dim_user = user.rename(columns={"user_id":"user_key"}).drop_duplicates(subset=["user_key"])

    dim_plan = plan.merge(
        plan_freq.rename(columns={"english_description":"payment_frequency_en","french_description":"payment_frequency_fr"}),
        on="payment_frequency_code", how="left"
    ).rename(columns={"plan_id":"plan_key","cost_amount":"plan_cost_amount"})

    dim_payment_method = user_payment_detail[["payment_method_code"]].drop_duplicates().reset_index(drop=True)
    dim_payment_method["payment_method_key"] = dim_payment_method.index + 1

    dim_channel = user_play_session[["channel_code"]].drop_duplicates().reset_index(drop=True)
    dim_channel["channel_key"] = dim_channel.index + 1

    dim_status = status_code.rename(columns={"play_session_status_code":"status_code","english_description":"status_en","french_description":"status_fr"})

    # Facts
    fps = user_play_session.copy()
    fps["start_datetime"] = to_naive_datetime(fps["start_datetime"])
    fps["end_datetime"] = to_naive_datetime(fps["end_datetime"])
    fps["duration_seconds"] = (fps["end_datetime"] - fps["start_datetime"]).dt.total_seconds()

    fact_play_session = (
        fps.merge(dim_channel, on="channel_code", how="left")
           .merge(dim_status[["status_code","status_en"]], on="status_code", how="left")
    )
    fact_play_session["start_date_key"] = fact_play_session["start_datetime"].dt.strftime("%Y%m%d")
    fact_play_session["end_date_key"] = fact_play_session["end_datetime"].dt.strftime("%Y%m%d")
    fact_play_session = fact_play_session.rename(columns={"user_id":"user_key","play_session_id":"play_session_key","total_score":"session_total_score"})

    fup = user_plan.rename(columns={"plan_id":"plan_key"})
    fup["start_date"] = to_naive_datetime(fup["start_date"])
    if "end_date" in fup.columns:
        fup["end_date"] = to_naive_datetime(fup["end_date"])
    fup = fup.merge(user_registration[["user_registration_id","user_id"]], on="user_registration_id", how="left").rename(columns={"user_id":"user_key"})
    fact_user_plan = fup.merge(dim_plan[["plan_key","plan_cost_amount","payment_frequency_code"]], on="plan_key", how="left")

    fact_payment_detail = user_payment_detail.merge(dim_payment_method, on="payment_method_code", how="left").rename(columns={"payment_detail_id":"payment_detail_key"})

    outdir.mkdir(exist_ok=True, parents=True)
    dim_user.to_csv(outdir/"dim_user.csv", index=False)
    dim_plan.to_csv(outdir/"dim_plan.csv", index=False)
    dim_payment_method.to_csv(outdir/"dim_payment_method.csv", index=False)
    dim_channel.to_csv(outdir/"dim_channel.csv", index=False)
    dim_status.to_csv(outdir/"dim_status.csv", index=False)
    fact_play_session.to_csv(outdir/"fact_play_session.csv", index=False)
    fact_user_plan.to_csv(outdir/"fact_user_plan.csv", index=False)
    fact_payment_detail.to_csv(outdir/"fact_payment_detail.csv", index=False)

def run_quality_checks(outdir: Path) -> pd.DataFrame:
    dim_user = pd.read_csv(outdir/"dim_user.csv")
    dim_plan = pd.read_csv(outdir/"dim_plan.csv")
    dim_payment_method = pd.read_csv(outdir/"dim_payment_method.csv")
    dim_channel = pd.read_csv(outdir/"dim_channel.csv")
    fact_play_session = pd.read_csv(outdir/"fact_play_session.csv")
    fact_user_plan = pd.read_csv(outdir/"fact_user_plan.csv")

    checks = []
    checks.append({"check":"dim_user PK unique","passed": dim_user["user_key"].is_unique})
    checks.append({"check":"dim_plan PK unique","passed": dim_plan["plan_key"].is_unique})
    checks.append({"check":"dim_payment_method PK unique","passed": dim_payment_method["payment_method_key"].is_unique})
    checks.append({"check":"dim_channel PK unique","passed": dim_channel["channel_key"].is_unique})
    checks.append({"check":"fact_play_session user_key not null","passed": fact_play_session["user_key"].notna().all()})
    checks.append({"check":"fact_user_plan plan_key exists","passed": fact_user_plan["plan_key"].isin(dim_plan["plan_key"]).all()})
    return pd.DataFrame(checks)

if __name__ == "__main__":
    excel = Path("DataSheet.xlsx")
    out = Path("star_schema")
    build_star_schema(excel, out)
    dq = run_quality_checks(out)
    dq.to_csv(out/"dq_checks.csv", index=False)
    print(dq)
