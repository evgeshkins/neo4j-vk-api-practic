import requests
import json
import sys
import os
from dotenv import load_dotenv

# Загрузка переменных окружения из файла .env
load_dotenv()

# Получаем VK access token из переменных окружения
VK_ACCESS_TOKEN = os.getenv('VK_ACCESS_TOKEN')
API_VERSION = '5.131'

def vk_api_request(method, params):
    params['access_token'] = VK_ACCESS_TOKEN
    params['v'] = API_VERSION
    url = f'https://api.vk.com/method/{method}'
    response = requests.get(url, params=params)
    return response.json()

def get_user_info(user_id):
    user_params = {
        'user_ids': user_id,
        'fields': 'followers_count'
    }
    response = vk_api_request('users.get', user_params)
    print(response)
    return response['response'][0] if 'response' in response else None

def get_followers(user_id):
    followers_params = {
        'user_id': user_id,
        'count': 1000
    }
    response = vk_api_request('friends.get', followers_params)
    return response['response']['items'] if 'response' in response else []

def get_subscriptions(user_id):
    subscriptions_params = {
        'user_id': user_id
    }
    response = vk_api_request('users.getSubscriptions', subscriptions_params)
    if 'response' in response:
        return {
            'users': response['response']['users']['items'],
            'groups': response['response']['groups']['items']
        }
    return {'users': [], 'groups': []}

def main(user_id):
    user_info = get_user_info(user_id)
    
    if user_info is None:
        print("Пользователь не найден.")
        return

    followers = get_followers(user_id)
    subscriptions = get_subscriptions(user_id)

    if not followers:
        print("У пользователя нет фолловеров.")
        return

    if not subscriptions['users'] and not subscriptions['groups']:
        print("У пользователя нет подписок.")
        return

    result = {
        'user_info': user_info,
        'followers': followers,
        'subscriptions': {
            'users': subscriptions['users'],
            'groups': subscriptions['groups']
        }
    }

    output_file = f'vk_user_{user_id}_data.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

    print(f"Данные сохранены в файл {os.path.abspath(output_file)}")

if __name__ == '__main__':
    if len(sys.argv) > 1:
        user_id = sys.argv[1]
    else:
        user_id = input("Введите ID пользователя ВК: ")

    main(user_id)
