import requests
import json

API_KEY ='3abbca0e1f9d50771d33fa9e8008c9bc'
city_name = 'Ha Noi'
city_id = 1581130

# Call api openweather with input is either city id or city name and return json
def openweathermap(city = 1581130):
    if str(city).isdigit():
        city_id = int(city)
        url = f'http://api.openweathermap.org/data/2.5/weather?id={city_id}&appid={API_KEY}'
    else:
        city_name = str(city)
        url = f'http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={API_KEY}'
    response = requests.get(url)

    if response.status_code == 200:
        weather_data = response.json()
        return weather_data
    else:
        print("Fail to call api openweather: status code " + str(response.status_code))

if __name__ == '__main__':
    data = openweathermap('Ha Noi')
    print(json.dumps(data))
    