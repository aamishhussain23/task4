from fastapi import FastAPI, Form, HTTPException, Request, Query, Body
from pydantic import BaseModel
from datetime import datetime
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from typing import Optional
import uvicorn

app = FastAPI()

class TokenData(BaseModel):
    token: str
    time: str

# Function to write data to Google Sheet using Google Sheets API.
def write_to_sheet(data: dict, credentials: Credentials):
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()
    spreadsheet_id = '1A8Mpe-dStdBKjc7DIc4WQSWRm-YK2KFB4o7qGVFBcY8'  # Replace with your actual spreadsheet ID
    range_name = 'Sheet1'  # Replace with your actual sheet name and range

    # Get the existing headers from the sheet
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name + '!1:1').execute()
    headers = result.get('values', [])[0] if result.get('values') else []

    # Find any new keys in the data
    # new_keys = [k for k in data.keys() if k not in headers]

    # Find any new keys in the data
    new_keys = [k for k in data.keys() if k not in headers and k not in ['token', 'param', 'time']]


    # If there are new keys, append them as new columns
    if new_keys:
        headers.extend(new_keys)
        sheet.values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name + '!1:1',
            valueInputOption='RAW',
            body={'values': [headers]}
        ).execute()

    # Append the data values in the same order as the headers
    values = [[data.get(h, '') for h in headers]]
    body = {
        'values': values
    }
    result = sheet.values().append(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption='RAW',
        body=body
    ).execute()

@app.post("/receive-data/{param:path}")
async def receive_data(
    param: str,
    request: Request,
    token: Optional[str] = Form(None),
    time: Optional[str] = Form(None),
    token_query: Optional[str] = Query(None, alias="token"),
    time_query: Optional[str] = Query(None, alias="time"),
    json_body: Optional[TokenData] = Body(None)
):
    # Initialize an empty data dictionary
    data = {}

    # Determine if data is coming in as form data, query parameters, or JSON payload
    if request.headers.get('content-type') == 'application/x-www-form-urlencoded':
        form_data = await request.form()
        data.update(form_data)
    elif request.headers.get('content-type') == 'application/json':
        json_data = await request.json()
        data.update(json_data)
    else:
        data.update(request.query_params)

    # Add token and time to the data
    data['token'] = data.get('token', token or token_query or (json_body.token if json_body else None))
    data['time'] = data.get('time', time or time_query or (json_body.time if json_body else None))
    data['param'] = param

    # Prepare the row data
    row_data = {
        "Order Code": 123456789,
        "Ticker": data.get("param", ""),
        "Sale Date": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "Customer Name": data.get("Customer Name", "Unknown"),
        "Gender": data.get("Gender", "Unknown"),
        "City": data.get("City", "Unknown"),
        "Order Amount": data.get("Order Amount", "0"),
    }

    # Add any additional data fields dynamically
    for key, value in data.items():
        if key not in row_data:
            row_data[key] = value

    # Load the credentials
    creds = Credentials(token=data['token'])

    # Write the row data to the sheet
    try:
        write_to_sheet(row_data, creds)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return row_data

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
