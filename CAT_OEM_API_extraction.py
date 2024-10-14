import requests
from dotenv import load_dotenv, find_dotenv
import os, json
import pandas as pd
load_dotenv(find_dotenv())

START_DATE_UTC = "2024-10-01T12:00:00Z"
END_DATE_UTC = "2024-10-02T12:00:00Z"

# ENDPOINTS GOTTEN FROM PUBLIC CAT API DOCUMENTATION AT https://digital.cat.com/knowledge-hub/articles/iso-15143-3-aemp-20-api-developer-guide#iso-15143-3-aemp-20-api-developer-guide
TOKEN_URL = 'https://fedlogin.cat.com/as/token.oauth2'
EQUIPMENT_URL = 'https://services.cat.com/telematics/iso15143/fleet/'
SNAPSHOT_URL  = 'https://services.cat.com/telematics/iso15143/fleet/{{pageNumber}}'
TS_LOCATIONS_URL = "https://api.cat.com/catDigital/iso15143/v1/fleet/equipment/makeModelSerial/{make}/{model}/{serialNumber}/locations/{startDateUTC}/{endDateUTC}/{pageNumber}"
TS_FUEL_RATIO_URL = "https://api.cat.com/catDigital/iso15143/v1/fleet/equipment/makeModelSerial/{make}/{model}/{serialNumber}/fuelRemainingRatio/{startDateUTC}/{endDateUTC}/{pageNumber}"
TS_FAULTS_URL = "https://api.cat.com/catDigital/iso15143/v1/fleet/equipment/makeModelSerial/{make}/{model}/{serialNumber}/faults/{startDateUTC}/{endDateUTC}/{pageNumber}"
TS_SWITCH_URL = "https://api.cat.com/catDigital/iso15143/v1/fleet/equipment/makeModelSerial/{make}/{model}/{serialNumber}/switchStatus/{startDateUTC}/{endDateUTC}/{pageNumber}"
TS_OPERATION_HOURS_URL = "https://api.cat.com/catDigital/iso15143/v1/fleet/equipment/makeModelSerial/{make}/{model}/{serialNumber}/cumulativeOperatingHours/{startDateUTC}/{endDateUTC}/{pageNumber}"
TS_IDLE_HOURS_URL = "https://api.cat.com/catDigital/iso15143/v1/fleet/equipment/makeModelSerial/{make}/{model}/{serialNumber}/cumulativeIdleHours/{startDateUTC}/{endDateUTC}/{pageNumber}"
TS_FUEL_USED_URL = "https://api.cat.com/catDigital/iso15143/v1/fleet/equipment/makeModelSerial/{make}/{model}/{serialNumber}/cumulativeFuelUsed/{startDateUTC}/{endDateUTC}/{pageNumber}"
TS_ENGINE_URL = "https://api.cat.com/catDigital/iso15143/v1/fleet/equipment/makeModelSerial/{make}/{model}/{serialNumber}/engineCondition/{startDateUTC}/{endDateUTC}/{pageNumber}"


def printSeparator(message):
    print("\n" + "="*50)
    print(message)
    print("="*50 + "\n")

def getAccessToken(client_id, client_secret) -> str:
    payload = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    response = requests.post(TOKEN_URL, data=payload, headers=headers)
    if response.status_code == 200:
        access_token = response.json().get('access_token')
        print("\033[92mGot Access Token\033[0m")
        return access_token
    else:
        print(f"\033[91mFailed to retrieve token, status code: {response.status_code}\033[0m")
        return response.text
    
def requestDataSnapShot(token:str, page_number:int) -> str:
    formated_url = SNAPSHOT_URL.replace('{{pageNumber}}', str(page_number))
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/iso15143-snapshot+json'
    }
    response = requests.get(formated_url, headers=headers)
    if response.status_code == 200:
        response = response.json()
        print("Successful Snapshot Request")
        #print(f"Retrieved page {response['Links'][0]['Href'].split('/')[-1]} of {response['Links'][1]['Href'].split('/')[-1]}")
        return(response)
    
    else:
        print(f"Failed to retrieve data, status code: {response.status_code}")
        return response.text
    

def requestDataTS(base_url:str, token:str, make:str, model:str, serialNumber:str,startDateUTC:str, endDateUTC:str,pageNumber:int) -> str:
    replaceDict = {
        '{make}': make,
        '{model}': model,
        '{serialNumber}': serialNumber,
        '{startDateUTC}': startDateUTC,
        '{endDateUTC}': endDateUTC,
        '{pageNumber}':str(pageNumber),
    }

    formated_url = multipleReplace(base_url, replaceDict)
    print(f"Requesting data from {formated_url}")
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': '*/*', 
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36'
    }
    response = requests.get(formated_url, headers=headers)

    if response.status_code == 200:
        response = response.json()
        print(f"Successful API Request")
        return(response)
    else:
        print(f"Failed to retrieve data, status code: {response.status_code}\n{response.text}")
        return response.text
    
def multipleReplace(text, dict):
    for key, value in dict.items():
        text = text.replace(key, value)
    return text

def getEquipmentAsList(token:str, snapshot_data) -> list[dict]:
    individual_equipment_snapshots:list[dict] = snapshot_data['Equipment']
    return [indiv['EquipmentHeader'] for indiv in individual_equipment_snapshots]


def getTotalDataPages(base_url:str, token:str, make:str=None, model:str=None, serialNumber:str=None,startDateUTC:str=None, endDateUTC:str=None) -> str:
    if base_url == 'https://services.cat.com/telematics/iso15143/fleet/{{pageNumber}}':
        data = requestDataSnapShot(token, 1)
    else: 
        data = requestDataTS(base_url, token, make, model, serialNumber,startDateUTC, endDateUTC, 1)
    #print(json.dumps(data, indent=4))
    try:
        links = data['Links']
        try:
            for link in links:
                if link["rel"] == "Last":
                    last_page_url = link["href"]
                    break
        except:
            for link in links:
                if link["Rel"] == "Last":
                    last_page_url = link["Href"]
                    break
        total_pages = last_page_url.split("/")[-1]
        print(f"Total Pages in Requested Data: {total_pages}")
        return int(total_pages)
    except:
        print("Failed to retrieve total pages")
        return 0

def extractEquipmentList(token:str) -> list[dict]:
    equipment_list = []
    snapshotTotalPages = getTotalDataPages(SNAPSHOT_URL, token)
    for page in range(1, snapshotTotalPages +1):
        snapshot_data = requestDataSnapShot(token, page)
        equipment_list_page = getEquipmentAsList(token, snapshot_data)
        equipment_list += equipment_list_page
        print(f"Retrieved page {page} of {snapshotTotalPages}")
    print(f"\033[92mTotal Equipment Retrieved: {len(equipment_list)}\033[0m")
    return equipment_list

def extractEquipmentTimeSeries(base_url:str, token:str, make:str, model:str, serialNumber:str, startDateUTC:str, endDateUTC:str, dataKey:str):
    total_data_list= []

    pages_equipment= getTotalDataPages(
        base_url= base_url,
        token=token,
        make= make,
        model= model, 
        serialNumber= serialNumber,
        startDateUTC=startDateUTC,
        endDateUTC=endDateUTC,
    )
    for page in range(1, pages_equipment +1):
        page_data = requestDataTS(
            base_url= base_url,
            token=token,
            make= make,
            model= model, 
            serialNumber= serialNumber,
            startDateUTC=startDateUTC,
            endDateUTC=endDateUTC,
            pageNumber=page
        )
        page_data = page_data[dataKey]
        total_data_list += page_data
    return pd.DataFrame(total_data_list)

#AUTENTICACION
printSeparator("OBTENIENDO TOKEN")
token = getAccessToken(os.getenv('CLIENT_ID'), os.getenv('CLIENT_SECRET'))

printSeparator("OBTENIENDO LISTA DE EQUIPOS")
equipment_list = extractEquipmentList(token)
df_equipment = pd.DataFrame(equipment_list)
print(df_equipment.head())

for equipment in equipment_list:
    printSeparator(f"Extracting Time Series Data for {equipment['OEMName']} {equipment['Model']} {equipment['SerialNumber']}")
    MAKE = equipment['OEMName']
    MODEL = equipment['Model']
    SERIAL_NUMBER = equipment['SerialNumber']

    #Location#
    try:
        printSeparator("Extracting Location Time Series")
        df_location = extractEquipmentTimeSeries(
            base_url= TS_LOCATIONS_URL,
            token=token,
            make= MAKE,
            model= MODEL, 
            serialNumber= SERIAL_NUMBER,
            startDateUTC=START_DATE_UTC,
            endDateUTC=END_DATE_UTC, 
            dataKey="Location"
        )
        df_location.to_csv(F"extracted_data/location_timeseries_example_{MAKE}_{MODEL}_{SERIAL_NUMBER}.csv", index=False)
        print("\033[92mLocation Time Series Extracted\033[0m")
        print(df_location.head())
    except:
        print("\033[91mFailed to extract Location Time Series\033[0m")

    #Fuel rem.#
    try:
        printSeparator("Extracting Fuel Remaining Time Series")
        df_fuel_remaining = extractEquipmentTimeSeries(
            base_url= TS_FUEL_RATIO_URL,
            token=token,
            make= MAKE,
            model= MODEL, 
            serialNumber= SERIAL_NUMBER,
            startDateUTC=START_DATE_UTC,
            endDateUTC=END_DATE_UTC, 
            dataKey="FuelRemaining"
        )
        df_fuel_remaining.to_csv(F"extracted_data/fuel_remaining_timeseries_example_{MAKE}_{MODEL}_{SERIAL_NUMBER}.csv", index=False)
        print("\033[92mFuel Remaining Time Series Extracted\033[0m")
        print(df_fuel_remaining.head())
    except:
        print("\033[91mFailed to extract Fuel Remaining Time Series\033[0m")
    
    #Faults#
    try:
        printSeparator("Extracting Fault Codes Time Series")
        df_fault_codes = extractEquipmentTimeSeries(
            base_url= TS_FAULTS_URL,
            token=token,
            make= MAKE,
            model= MODEL, 
            serialNumber= SERIAL_NUMBER,
            startDateUTC=START_DATE_UTC,
            endDateUTC=END_DATE_UTC, 
            dataKey="FaultCode",
        )
        df_fault_codes.to_csv(F"extracted_data/fault_codes_timeseries_example_{MAKE}_{MODEL}_{SERIAL_NUMBER}.csv", index=False)
        print("\033[92mFault Codes Time Series Extracted\033[0m")
        print(df_fault_codes.head())
    except:
        print("\033[91mFailed to extract Fault Codes Time Series\033[0m")

    #Operation Hours#
    try: 
        printSeparator("Extracting Operation Hours Time Series")
        df_operation_hrs = extractEquipmentTimeSeries(
            base_url= TS_OPERATION_HOURS_URL,
            token=token,
            make= MAKE,
            model= MODEL, 
            serialNumber= SERIAL_NUMBER,
            startDateUTC=START_DATE_UTC,
            endDateUTC=END_DATE_UTC, 
            dataKey="CumulativeOperatingHours",
        )
        df_operation_hrs.to_csv(F"extracted_data/operation_hrs_timeseries_example_{MAKE}_{MODEL}_{SERIAL_NUMBER}.csv", index=False)
        print("\033[92mOperation Hours Time Series Extracted\033[0m")
        print(df_operation_hrs.head())
    except:
        print("\033[91mFailed to extract Operation Hours Time Series\033[0m")

    #Idle Hours#
    try:
        printSeparator("Extracting Idle Hours Time Series")
        df_idle_hrs = extractEquipmentTimeSeries(
            base_url= TS_IDLE_HOURS_URL,
            token=token,
            make= MAKE,
            model= MODEL, 
            serialNumber= SERIAL_NUMBER,
            startDateUTC=START_DATE_UTC,
            endDateUTC=END_DATE_UTC, 
            dataKey="CumulativeIdleHours",
        )
        df_idle_hrs.to_csv(F"extracted_data/idle_hrs_timeseries_example_{MAKE}_{MODEL}_{SERIAL_NUMBER}.csv", index=False)
        print("\033[92mIdle Hours Time Series Extracted\033[0m")
        print(df_idle_hrs.head())
    except:
        print("\033[91mFailed to extract Idle Hours Time Series\033[0m")

    #Fuel Used#
    try:
        printSeparator("Extracting Fuel Consumed Time Series")
        df_fuel_consumed = extractEquipmentTimeSeries(
            base_url= TS_FUEL_USED_URL,
            token=token,
            make= MAKE,
            model= MODEL, 
            serialNumber= SERIAL_NUMBER,
            startDateUTC=START_DATE_UTC,
            endDateUTC=END_DATE_UTC, 
            dataKey="FuelUsed",
        )
        df_fuel_consumed.to_csv(F"extracted_data/fuel_consumed_timeseries_example_{MAKE}_{MODEL}_{SERIAL_NUMBER}.csv", index=False)
        print("\033[92mFuel Consumed Time Series Extracted\033[0m")
        print(df_fuel_consumed.head())
    except:
        print("\033[91mFailed to extract Fuel Consumed Time Series\033[0m")

    #Engine Condition#
    try:
        printSeparator("Extracting Engine Condition Time Series")
        df_engine_condition = extractEquipmentTimeSeries(
            base_url= TS_ENGINE_URL,
            token=token,
            make= MAKE,
            model= MODEL, 
            serialNumber= SERIAL_NUMBER,
            startDateUTC=START_DATE_UTC,
            endDateUTC=END_DATE_UTC, 
            dataKey="EngineStatus",
        )
        df_engine_condition.to_csv(F"extracted_data/engine_timeseries_example_{MAKE}_{MODEL}_{SERIAL_NUMBER}.csv", index=False)
        print("\033[92mEngine Condition Time Series Extracted\033[0m")
        print(df_engine_condition.head())
    except Exception as e:
        print("\033[91mFailed to extract Engine Condition Time Series\033[0m")
        print(e)

    #Switch Status#
    try:
        printSeparator("Extracting Switch Status Time Series")
        df_switch = extractEquipmentTimeSeries(
            base_url= TS_SWITCH_URL,
            token=token,
            make= MAKE,
            model= MODEL, 
            serialNumber= SERIAL_NUMBER,
            startDateUTC=START_DATE_UTC,
            endDateUTC=END_DATE_UTC, 
            dataKey="SwitchStatus",
        )
        df_switch.to_csv(F"extracted_data/switches_timeseries_example_{MAKE}_{MODEL}_{SERIAL_NUMBER}.csv", index=False)
        print("\033[92mSwitch Status Time Series Extracted\033[0m")
        print(df_switch.head())
    except:
        print("\033[91mFailed to extract Switch Status Time Series\033[0m")

printSeparator("\033[92mExtraction Complete\033[0m")

