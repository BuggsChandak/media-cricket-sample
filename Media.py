import boto3
import math
import os

session = boto3.Session(profile_name='default')
client_rekognition = session.client('rekognition')
client_dynamodb = boto3.resource('dynamodb')
table_name = 'media-cricket'
table = client_dynamodb.Table(table_name)

def detect_text(file_names):

    start_time_secs = 0
    end_time_secs = 30
    frame=1
    prev_over=''

    for file in file_names:
        print(file)
        with open(r'C:\Users\bschandk\Documents\SA\ML\input' + '\\' + file,'rb') as image_file:
            image = image_file.read()

        response = client_rekognition.detect_text(Image={'Bytes': image})
        textDetections = response['TextDetections']
        print('Detected text\n----------')
        for text in textDetections:
            if ((math.trunc(text['Geometry']['BoundingBox']['Left'] * 100 ) / 100) == 0.33 and (math.trunc(text['Geometry']['BoundingBox']['Top'] * 100) / 100) == 0.86):
                id=text['Id']
                if 'ParentId' in text:
                    parent_id = text['ParentId']
                    if id==parent_id:
                        continue
                else:
                    print('Detected text:' + text['DetectedText'])
                    conf=format(text['Confidence']) + "%"
                    text_over=text['DetectedText']
                    if "." in text_over:
                        parts = text_over.split(".")
                        over = parts[0]
                        print("Over:", over)
                        ball_no = parts[1]
                        print("Ball_no:", ball_no)
                    else:
                        over = text_over
                        print("Over:", over)
                        ball_no = '0'
                        print("Ball_no:", ball_no)

        if (prev_over==text_over):
            frame=frame+1
            start_time_secs=start_time_secs+30
            end_time_secs=end_time_secs+30
            print ("Hello" + prev_over+" "+text_over)
        else:
            data = {
            'frame': frame,
            'over_no': over,
            'ball_no': ball_no,
            'start_time_secs': start_time_secs,
            'end_time_secs': end_time_secs,
            'confidence': conf
        }
#            table.put_item(Item=data)
            print("Data inserted into DynamoDB table")
            print(len(textDetections))
            print ("Hi1" + prev_over+" "+text_over)
            print (data)
            start_time_secs=start_time_secs+30
            end_time_secs=end_time_secs+30
            prev_over=text_over
            frame=frame+1
        print()
        
def main():
    file_names = []
    folder_path = r'C:\Users\bschandk\Documents\SA\ML\input'
    
    for _, _, files in os.walk(folder_path):
        file_names.extend(files)

    detect_text(file_names)
    print("done")


if __name__ == "__main__":
    main()