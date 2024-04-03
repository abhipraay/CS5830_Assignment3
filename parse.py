
## parsing the csv
def parseCSV(data):
    df = data.split('","')
    df[0] = df[0].strip('"')
    df[-1] = df[-1].strip('"')
    return list(df)