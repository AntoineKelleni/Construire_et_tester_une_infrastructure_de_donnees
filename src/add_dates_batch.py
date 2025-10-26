
from pathlib import Path
from datetime import datetime, time
import pandas as pd

def parse_date_token(token: str):
    token = token.strip()
    patterns = ["%d%m%y", "%d%m%Y", "%Y%m%d", "%d-%m-%y", "%d/%m/%y", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%Y/%m/%d"]
    for fmt in patterns:
        try:
            return datetime.strptime(token, fmt).date()
        except ValueError:
            continue
    return None

def coerce_time(value):
    if pd.isna(value):
        return None
    if isinstance(value, time):
        return value
    if isinstance(value, datetime):
        return value.time()
    if isinstance(value, (int, float)):
        try:
            seconds = int(value)
            h = seconds // 3600
            m = (seconds % 3600) // 60
            s = seconds % 60
            return time(hour=h % 24, minute=m, second=s)
        except Exception:
            return None
    s = str(value).strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(s, fmt).time()
        except ValueError:
            continue
    return None

def process_one(in_path: Path, out_path: Path, engine: str = "openpyxl"):
    xls = pd.ExcelFile(in_path, engine=engine)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine=engine) as writer:
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(in_path, sheet_name=sheet_name, engine=engine)

            # Parse the date *once* from the sheet name (first token like "071024")
            parsed_date = parse_date_token(sheet_name.split()[0])

            # 1) Date = ISO YYYY-MM-DD if parsed, otherwise keep raw name
            if parsed_date is not None:
                df["Date"] = parsed_date.isoformat()
            else:
                df["Date"] = sheet_name
                print(f"[WARN] {in_path.name} | '{sheet_name}': nom d'onglet non parsable -> 'Date' laissée telle quelle.")

            # 2) DateTime = parsed date + Time (if possible), placed after 'Time'
            if "Time" in df.columns and parsed_date is not None:
                dt_values = []
                for v in df["Time"]:
                    tv = coerce_time(v)
                    if tv is not None:
                        dt_values.append(datetime.combine(parsed_date, tv).isoformat())
                    else:
                        dt_values.append(pd.NA)
                time_idx = df.columns.get_loc("Time")
                df.insert(time_idx + 1, "DateTime", dt_values)
                print(f"[OK] {in_path.name} | '{sheet_name}': 'Date' ISO et 'DateTime' à côté de 'Time'.")
            else:
                if "Time" not in df.columns:
                    print(f"[INFO] {in_path.name} | '{sheet_name}': 'Date' mise à jour. Pas de 'Time' -> pas de 'DateTime'.")
                elif parsed_date is None:
                    print(f"[WARN] {in_path.name} | '{sheet_name}': 'DateTime' non créée (date non parsable).")

            df.to_excel(writer, sheet_name=sheet_name, index=False)
    print(f"||OK|| Écrit: {out_path}")

def main():
    # Suppose ce script est placé dans src/ et le projet a la forme ../data/brut
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    in_dir = project_root / "data" / "brut"
    out_dir = project_root / "data" / "brut_with_dates_and_times"
    out_dir.mkdir(parents=True, exist_ok=True)

    excel_files = sorted(in_dir.glob("*.xlsx"))
    if not excel_files:
        print(f"||WARN|| Aucun .xlsx trouvé dans {in_dir}")
        return

    for in_path in excel_files:
        out_path = out_dir / (in_path.stem + "_with_date_time" + in_path.suffix)
        try:
            process_one(in_path, out_path, engine="openpyxl")
        except Exception as e:
            print(f"||ERROR|| {in_path.name}: {e}")

if __name__ == "__main__":
    main()
