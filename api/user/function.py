from transformer.controller import Csvgres

def create_or_update_user(user_id, timestamp):
    csv_db = Csvgres()
    csv_db.connect_database(f"c users")
    results = csv_db.select(f"SELECT * FROM users WHERE user_id = '{user_id}'").to_dict(orient='records')
    if results:
        csv_db.update(f"UPDATE users SET last_active = '{timestamp}' WHERE user_id = '{user_id}'")
    else:
        csv_db.insert(f"INSERT INTO users (user_id, last_active) VALUES ('{user_id}', '{timestamp}')")
    return csv_db.select(f"SELECT * FROM users WHERE user_id = '{user_id}'")