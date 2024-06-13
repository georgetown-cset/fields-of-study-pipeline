import dataset

db = dataset.connect('sqlite:///data/wiki.db')

page_table = db['pages']
ref_table = db['refs']

def main():
    for i, record in enumerate(db.query("SELECT COUNT(DISTINCT field) FROM refs where en_text IS NOT NULL"), 1):
        non_null = list(record.values())[0]
    for i, record in enumerate(db.query("SELECT COUNT(DISTINCT field) FROM refs"), 1):
        total = list(record.values())[0]
    print(f"There are {total} records in the refs table, and of those, {non_null} have at least one reference.")

if __name__ == "__main__":
    main()