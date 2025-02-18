from transformer.controller import Csvgres

async def create_or_update_user(user_id: str, user_email: str):
    csv_db = Csvgres()
    results = await csv_db.select(f"SELECT * FROM users WHERE userid = '{user_id}';", 'csvgres')
    if not results.empty:
        print('results', results)
    await csv_db.insert(f"INSERT INTO users (userid, useremail) VALUES ('{user_id}', '{user_email}');", 'csvgres')
    results = await csv_db.select(f"SELECT * FROM users WHERE userid = '{user_id}';", 'csvgres')
    return results