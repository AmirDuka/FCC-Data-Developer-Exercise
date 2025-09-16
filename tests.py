
import pandas as pd
from pathlib import Path
from main import build_star_schema, run_quality_checks

def test_pipeline(tmp_path: Path = Path('tmp_test')):
    excel = Path('DataSheet.xlsx')
    out = tmp_path
    build_star_schema(excel, out)
    assert (out/'dim_user.csv').exists()
    assert (out/'fact_play_session.csv').exists()
    dq = run_quality_checks(out)
    assert dq['passed'].all()

if __name__ == '__main__':
    test_pipeline()
    print('All tests passed!')
