from utils.database import get_db

async def create_or_update_user(user_id: str, user_email: str):
    csv_db = get_db()
    results = await csv_db.select(f"SELECT * FROM users WHERE userid = '{user_id}';", 'csvgres')
    if not results:
        await csv_db.insert(f"INSERT INTO users (userid, useremail) VALUES ('{user_id}', '{user_email}');", 'csvgres')
    results = await csv_db.select(f"SELECT * FROM users WHERE userid = '{user_id}';", 'csvgres')
    return results