"""Generate synthetic Phorest-style CSV and date table for pipeline tests."""
import os
import pandas as pd

# Phorest CSV: first col = service, second = metric, then employee columns, then one column dropped by process_sheets
# Row 0 is dropped by process_sheets (db.drop(0)), so we need a dummy first row
HEADER = "service,metric,Kiara Alcaraz,Diamond Antley,DropCol"
ROWS = [
    "dummy,dummy,0,0,0",  # dropped row
    "Series,serviceCategoryAmount,150.50,80.25,0",  # Hair
    "Products,productsAmount,30,45,0",  # Retail
    "B3 Treatments,serviceCategoryAmount,900,400,0",  # Treatment (900 > 800 for cat 4)
    "Bridal,courseServiceAmount,200,100,0",  # Makeup
    "TipsRow,tips,25,15,0",
]
CSV_CONTENT = "\n".join([HEADER] + ROWS)

def write_test_csv(path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(CSV_CONTENT)

def write_test_dates(path: str, first="2024-11-10", last="2024-11-16") -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    pd.DataFrame([[first, last]], columns=["first_day", "last_day"]).to_excel(path, index=False)

if __name__ == "__main__":
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    write_test_csv(os.path.join(base, "branch_data", "TestBranch.csv"))
    write_test_dates(os.path.join(base, "work_new", "Tabela_Datas.xlsx"))
    print("Wrote TestBranch.csv and Tabela_Datas.xlsx")
