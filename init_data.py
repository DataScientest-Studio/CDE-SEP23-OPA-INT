import db_driver as db
import cross_cutting.symbol_repository as sr

if __name__ == "__main__":
    db.create_db()
    symbol_repo = sr.SymbolRepository()
    symbol_repo.add_symbol("ETHEUR")

