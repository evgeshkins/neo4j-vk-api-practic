import requests
import logging
from dotenv import load_dotenv
import os
from neo4j import GraphDatabase
import sys

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


# Функция для выполнения запроса к VK API
def vk_api_request(method, params):
    params['access_token'] = VK_ACCESS_TOKEN
    params['v'] = API_VERSION
    params['lang'] = 'ru'
    api_url = f'https://api.vk.com/method/{method}'
    response = requests.get(api_url, params=params)

    if response.status_code == 200:
        data = response.json()
        if 'error' in data:
            logger.error(f"VK API error: {data['error']['error_msg']}")
            return None
        return data['response']
    else:
        logger.error(f"HTTP error: {response.status_code} - {response.text}")
        return None



def close_driver():
    driver.close()


def get_user_data(user_id):
    params = {
        "user_ids": user_id,
        "fields": "first_name,last_name,sex,home_town,city,screen_name"
    }
    return vk_api_request("users.get", params)


def get_followers(user_id):
    params = {
        "user_id": user_id
    }
    return vk_api_request("users.getFollowers", params)


def get_followers_info(follower_ids):
    params = {
        "user_ids": ",".join(map(str, follower_ids)),
        "fields": "first_name,last_name,sex,home_town,city,screen_name"
    }
    return vk_api_request("users.get", params)


def get_subscriptions(user_id):
    params = {
        "user_id": user_id,
        "extended": 1
    }
    return vk_api_request("users.getSubscriptions", params)


def get_groups_info(group_ids):
    params = {
        "group_ids": ",".join(map(str, group_ids)),
        "fields": "name,screen_name"
    }
    return vk_api_request("groups.getById", params)


def save_user(tx, user):
    city = user.get('city', {}).get('title', '')
    home_town = user.get('home_town', '') or city

    tx.run(
        """
        MERGE (u:User {id: $id})
        SET u.screen_name = $screen_name,
            u.name = $name,
            u.sex = $sex,
            u.home_town = $home_town
        """,
        id=user['id'],
        screen_name=user.get('screen_name', ''),
        name=f"{user.get('first_name', '')} {user.get('last_name', '')}",
        sex=user.get('sex', ''),
        home_town=home_town
    )


def save_group(tx, group):
    tx.run(
        """
        MERGE (g:Group {id: $id})
        SET g.name = $name, g.screen_name = $screen_name
        """,
        id=group['id'],
        name=group.get('name', ''),
        screen_name=group.get('screen_name', '')
    )


def create_relationship(tx, user_id, target_id, relationship_type):
    tx.run(
        f"""
        MATCH (u:User {{id: $user_id}})
        MATCH (target {{id: $target_id}})
        MERGE (u)-[:{relationship_type}]->(target)
        """,
        user_id=user_id, target_id=target_id
    )
    logger.info(f"Связь:  {user_id} - [ {relationship_type} ] - {target_id}")


def process_user(user_id, level, max_depth, max_users=100):
    queue = [(user_id, level)]
    visited = set()
    processed_count = 0

    while queue:
        current_id, current_level = queue.pop(0)

        if current_id in visited or current_level > max_depth:
            continue
        visited.add(current_id)
        processed_count += 1

        # Прерываем, если достигли лимита на количество узлов
        if processed_count > max_users:
            logger.info("Максимальное кол-во пользователей достигнуто.")
            break

        user_data = get_user_data(current_id)
        if user_data is None:
            logger.info(f"Не удалось получить данные для пользователя {current_id} (private)")
            continue
        user_info = user_data[0]

        with driver.session() as session:
            session.execute_write(save_user, user_info)
            logger.info(f"Добавлен пользователь {user_info['id']}")

            # Получаем и сохраняем фолловеров
            followers_data = get_followers(current_id)
            if followers_data:
                follower_ids = followers_data['items']
                followers_info = get_followers_info(follower_ids)
                for follower in followers_info:
                    if follower['id'] not in visited:
                        session.execute_write(save_user, follower)
                        session.execute_write(create_relationship, follower['id'], current_id, "Follow")
                        queue.append((follower['id'], current_level + 1))
                        logger.info(f"Добавлен фолловер {follower['id']} для пользователя {current_id}")

            # Получаем и сохраняем подписки
            subscriptions_data = get_subscriptions(current_id)
            if subscriptions_data and 'items' in subscriptions_data:
                user_group_ids = [sub['id'] for sub in subscriptions_data['items'] if sub.get('type') == 'page']
                if user_group_ids:
                    groups_info = get_groups_info(user_group_ids)
                    for group in groups_info:
                        if group['id'] not in visited:
                            session.execute_write(save_group, group)
                            session.execute_write(create_relationship, current_id, group['id'], "Subscribe")
                            logger.info(f"Добавлена подписка на группу {group['id']} для пользователя {current_id}")

        logger.info(f"Глубина: {current_level}. Пользователь: {current_id}.\n")

    logger.info("Обработка фолловеров и подписок завершена.")


def main(input_user_id):
    if not VK_ACCESS_TOKEN:
        logger.error("Токен VK API не задан")
        return

    user_data = get_user_data(input_user_id)
    logger.info(f"Полученные данные пользователя: {user_data}")

    if user_data:
        user_info = user_data[0]
        user_id = user_info['id']
        max_depth = 2
        process_user(user_id, 0, max_depth)
    else:
        logger.error("Не удалось получить данные пользователя")


    close_driver()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_user_id = sys.argv[1]
    else:
        input_user_id = input("Введите ID пользователя ВК (либо '-' , чтобы взять шаблон ID): ")
        if input_user_id == '-':
            main('183170347')
        else:
            main(input_user_id)
