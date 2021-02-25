import requests
import json

def get_data():

    url = 'https://data.cityofnewyork.us/resource/hvrh-b6nb.json'
    response = requests.get(url)

    data = json.loads(response.content)

    with open('out/resource_hvrh-b6nb.json', 'w') as f:
        for j in data:
            f.write(json.dumps(j)+'\n')

    
if __name__ == '__main__':
    get_data()


