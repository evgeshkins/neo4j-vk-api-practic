import logging
from dotenv import load_dotenv
import os
from neo4j import GraphDatabase
import argparse

# Загрузка переменных из файла .env
load_dotenv()

# настройки логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Получаем VK access token и данные для создания движка бд из переменных окружения
VK_ACCESS_TOKEN = os.getenv('VK_ACCESS_TOKEN')
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
API_VERSION = '5.131'

url = "bolt://localhost:7687"
driver = GraphDatabase.driver(url, auth=(DB_USER, DB_PASSWORD))

# Получение кол-ва всех пользователей
def get_users_count():
    query = "MATCH (u:User) RETURN count(u) AS total_users"
    with driver.session() as session:
        result = session.run(query)
        total_users = result.single()["total_users"]
        print("Total Users:", total_users)


# Получение кол-ва всех групп
def get_groups_count():
    query = "MATCH (g:Group) RETURN count(g) AS total_groups"
    with driver.session() as session:
        result = session.run(query)
        total_groups = result.single()["total_groups"]
        print("Total Groups:", total_groups)


# Получаем топ-5 пользователей по кол-ву фолловеров
def get_top_5_users_by_followers_count():
    query = """
    MATCH (u:User)<-[:Follow]-(follower:User)
    RETURN u.name AS name, count(follower) AS followers_count
    ORDER BY followers_count DESC
    LIMIT 5
    """
    with driver.session() as session:
        result = session.run(query)
        print("Top 5 Users by Followers:")
        for record in result:
            print(f"Name: {record['name']}, Followers Count: {record['followers_count']}")


# Получаем топ-5 самых популярных групп
def get_top_5_most_popular_groups():
    query = """
    MATCH (g:Group)<-[:Subscribe]-(u:User)
    RETURN g.name AS group_name, count(u) AS subscribers_count
    ORDER BY subscribers_count DESC
    LIMIT 5
    """
    with driver.session() as session:
        result = session.run(query)
        print("Top 5 Popular Groups:")
        for record in result:
            print(f"Group Name: {record['group_name']}, Subscribers Count: {record['subscribers_count']}")


# Получаем всех пользователей, которые фолловеры друг друга
def get_joint_followers():
    query = """
    MATCH (u1:User)-[:Follow]->(u2:User), (u2)-[:Follow]->(u1)
    RETURN u1.name AS user1_name, u2.name AS user2_name
    """
    with driver.session() as session:
        result = session.run(query)
        print("Пользователи, подписанные совместно:")
        for record in result:
            print(f"User1: {record['user1_name']} is a mutual follower with User2: {record['user2_name']}")



def main():
    parser = argparse.ArgumentParser(description="Скрипт для получения результатов запроса")
    parser.add_argument('--total_users', action='store_true', help="Получение кол-ва всех пользователей")
    parser.add_argument('--total_groups', action='store_true', help="Получение кол-ва всех групп")
    parser.add_argument('--top_users', action='store_true', help="Получение топ-5 пользователей по кол-ву фолловеров")
    parser.add_argument('--top_groups', action='store_true', help="Получение топ-5 самых популярных групп")
    parser.add_argument('--joint_followers', action='store_true', help="Получение всех пользователей, которые фолловеры друг друга")

    args = parser.parse_args()

    if args.total_users:
        get_users_count()
    if args.total_groups:
        get_groups_count()
    if args.top_users:
        get_top_5_users_by_followers_count()
    if args.top_groups:
        get_top_5_most_popular_groups()
    if args.joint_followers:
        get_joint_followers()


if __name__ == "__main__":
    main()