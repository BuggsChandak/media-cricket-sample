import boto3
import os
import json
import argparse
from pathlib import Path
from datetime import datetime

class ImageRecognitionApp:
    """
    A general-purpose image recognition application using AWS Rekognition.
    This app can detect objects, scenes, faces, text, and more in images.
    """
    
    def __init__(self, profile_name='default', region_name=None):
        """
        Initialize the ImageRecognitionApp with AWS credentials.
        
        Args:
            profile_name (str): AWS profile name to use
            region_name (str): AWS region to use (optional)
        """
        self.session = boto3.Session(profile_name=profile_name, region_name=region_name)
        self.rekognition = self.session.client('rekognition')
        
        # For backward compatibility, initialize DynamoDB connection
        self.dynamodb = self.session.resource('dynamodb')
        self.cricket_table_name = 'media-cricket'
        
    def load_image(self, image_path):
        """
        Load an image from a file path.
        
        Args:
            image_path (str): Path to the image file
            
        Returns:
            bytes: The image data as bytes
        """
        with open(image_path, 'rb') as image_file:
            return image_file.read()
    
    def detect_labels(self, image_data, max_labels=10, min_confidence=70):
        """
        Detect objects and scenes in an image.
        
        Args:
            image_data (bytes): Image data as bytes
            max_labels (int): Maximum number of labels to return
            min_confidence (float): Minimum confidence level for labels
            
        Returns:
            dict: The response from AWS Rekognition
        """
        response = self.rekognition.detect_labels(
            Image={'Bytes': image_data},
            MaxLabels=max_labels,
            MinConfidence=min_confidence
        )
        return response
    
    def detect_faces(self, image_data, attributes=['ALL']):
        """
        Detect faces and their attributes in an image.
        
        Args:
            image_data (bytes): Image data as bytes
            attributes (list): List of attributes to detect
            
        Returns:
            dict: The response from AWS Rekognition
        """
        response = self.rekognition.detect_faces(
            Image={'Bytes': image_data},
            Attributes=attributes
        )
        return response
    
    def detect_text(self, image_data):
        """
        Detect text in an image.
        
        Args:
            image_data (bytes): Image data as bytes
            
        Returns:
            dict: The response from AWS Rekognition
        """
        response = self.rekognition.detect_text(
            Image={'Bytes': image_data}
        )
        return response
    
    def recognize_celebrities(self, image_data):
        """
        Recognize celebrities in an image.
        
        Args:
            image_data (bytes): Image data as bytes
            
        Returns:
            dict: The response from AWS Rekognition
        """
        response = self.rekognition.recognize_celebrities(
            Image={'Bytes': image_data}
        )
        return response
    
    def detect_moderation_labels(self, image_data, min_confidence=50):
        """
        Detect unsafe content in an image.
        
        Args:
            image_data (bytes): Image data as bytes
            min_confidence (float): Minimum confidence level for labels
            
        Returns:
            dict: The response from AWS Rekognition
        """
        response = self.rekognition.detect_moderation_labels(
            Image={'Bytes': image_data},
            MinConfidence=min_confidence
        )
        return response
    
    def analyze_image(self, image_path, features=None):
        """
        Analyze an image using multiple recognition features.
        
        Args:
            image_path (str): Path to the image file
            features (list): List of features to detect. If None, all features are used.
                            Options: 'labels', 'faces', 'text', 'celebrities', 'moderation'
            
        Returns:
            dict: Combined results from all requested features
        """
        if features is None:
            features = ['labels', 'text']
        
        image_data = self.load_image(image_path)
        results = {'image_path': image_path, 'timestamp': datetime.now().isoformat()}
        
        if 'labels' in features:
            results['labels'] = self.detect_labels(image_data)
        
        if 'faces' in features:
            results['faces'] = self.detect_faces(image_data)
        
        if 'text' in features:
            results['text'] = self.detect_text(image_data)
        
        if 'celebrities' in features:
            results['celebrities'] = self.recognize_celebrities(image_data)
        
        if 'moderation' in features:
            results['moderation'] = self.detect_moderation_labels(image_data)
        
        return results
    
    def save_results(self, results, output_path=None):
        """
        Save analysis results to a JSON file.
        
        Args:
            results (dict): Analysis results
            output_path (str): Path to save the results. If None, a default path is used.
            
        Returns:
            str: Path where results were saved
        """
        if output_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"image_analysis_{timestamp}.json"
            output_path = os.path.join(os.getcwd(), filename)
        
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        return output_path
    
    def print_analysis_summary(self, results):
        """
        Print a human-readable summary of the analysis results.
        
        Args:
            results (dict): Analysis results
        """
        print(f"\n===== Image Analysis Summary =====")
        print(f"Image: {results.get('image_path', 'Unknown')}")
        print(f"Analysis Time: {results.get('timestamp', 'Unknown')}")
        
        if 'labels' in results:
            print("\n----- Objects and Scenes -----")
            for label in results['labels'].get('Labels', []):
                print(f"- {label['Name']} ({label['Confidence']:.2f}%)")
                
        if 'text' in results:
            print("\n----- Detected Text -----")
            for text in results['text'].get('TextDetections', []):
                if text.get('Type') == 'LINE':
                    print(f"- \"{text['DetectedText']}\" ({text['Confidence']:.2f}%)")
        
        if 'faces' in results:
            print("\n----- Detected Faces -----")
            for i, face in enumerate(results['faces'].get('FaceDetails', []), 1):
                gender = face.get('Gender', {}).get('Value', 'Unknown')
                age_low = face.get('AgeRange', {}).get('Low', 0)
                age_high = face.get('AgeRange', {}).get('High', 0)
                emotions = [e['Type'] for e in face.get('Emotions', []) if e['Confidence'] > 50]
                emotion_str = ', '.join(emotions) if emotions else 'None detected'
                print(f"- Face #{i}: {gender}, Age {age_low}-{age_high}, Emotions: {emotion_str}")
        
        if 'celebrities' in results:
            print("\n----- Celebrities -----")
            for celeb in results['celebrities'].get('CelebrityFaces', []):
                print(f"- {celeb['Name']} ({celeb['MatchConfidence']:.2f}%)")
            
        if 'moderation' in results:
            print("\n----- Content Moderation -----")
            for label in results['moderation'].get('ModerationLabels', []):
                print(f"- {label['Name']} ({label['Confidence']:.2f}%)")
        
        print("\n=================================")

    def process_cricket_specific(self, image_data):
        """
        Process cricket-specific text detection (legacy functionality).
        
        Args:
            image_data (bytes): Image data as bytes
            
        Returns:
            dict: Cricket-specific analysis results
        """
        response = self.rekognition.detect_text(Image={'Bytes': image_data})
        textDetections = response['TextDetections']
        
        results = {
            'text_detections': textDetections,
            'cricket_data': None
        }
        
        # Look for cricket-specific text at the expected position
        for text in textDetections:
            if ((round(text['Geometry']['BoundingBox']['Left'] * 100) / 100) == 0.33 and 
                (round(text['Geometry']['BoundingBox']['Top'] * 100) / 100) == 0.86):
                
                if 'ParentId' in text and text['Id'] == text['ParentId']:
                    continue
                
                detected_text = text['DetectedText']
                confidence = text['Confidence']
                
                cricket_data = {
                    'text': detected_text,
                    'confidence': confidence
                }
                
                # Parse over and ball information
                if "." in detected_text:
                    parts = detected_text.split(".")
                    cricket_data['over'] = parts[0]
                    cricket_data['ball'] = parts[1]
                else:
                    cricket_data['over'] = detected_text
                    cricket_data['ball'] = '0'
                
                results['cricket_data'] = cricket_data
                break
        
        return results
    
    def save_to_dynamodb(self, data, table_name=None):
        """
        Save data to DynamoDB (for backward compatibility).
        
        Args:
            data (dict): Data to save
            table_name (str): DynamoDB table name. If None, uses the default cricket table.
            
        Returns:
            dict: Response from DynamoDB
        """
        if table_name is None:
            table_name = self.cricket_table_name
        
        table = self.dynamodb.Table(table_name)
        response = table.put_item(Item=data)
        return response


def main():
    """
    Main function to run the image recognition app from the command line.
    """
    parser = argparse.ArgumentParser(description='Image Recognition Application')
    parser.add_argument('--image', '-i', help='Path to the image file or directory of images')
    parser.add_argument('--features', '-f', nargs='+', 
                        choices=['labels', 'faces', 'text', 'celebrities', 'moderation'],
                        default=['labels', 'text'],
                        help='Features to detect in the image')
    parser.add_argument('--output', '-o', help='Path to save the results')
    parser.add_argument('--profile', '-p', default='default', help='AWS profile name')
    parser.add_argument('--region', '-r', help='AWS region name')
    parser.add_argument('--cricket', action='store_true', help='Use cricket-specific analysis')
    
    args = parser.parse_args()
    
    # Initialize the app
    app = ImageRecognitionApp(profile_name=args.profile, region_name=args.region)
    
    # Process a single image or a directory of images
    image_path = args.image if args.image else input("Enter the path to an image or directory: ")
    path = Path(image_path)
    
    if path.is_file():
        # Process a single image
        if args.cricket:
            # Use cricket-specific functionality
            image_data = app.load_image(str(path))
            results = app.process_cricket_specific(image_data)
            
            if results['cricket_data']:
                print(f"Cricket data detected: Over {results['cricket_data']['over']}, Ball {results['cricket_data']['ball']}")
                print(f"Confidence: {results['cricket_data']['confidence']}%")
            else:
                print("No cricket data detected in the image.")
        else:
            # Use general image recognition
            results = app.analyze_image(str(path), features=args.features)
            app.print_analysis_summary(results)
            
            if args.output:
                output_path = app.save_results(results, args.output)
                print(f"Results saved to: {output_path}")
    
    elif path.is_dir():
        # Process all images in a directory
        image_extensions = ['.jpg', '.jpeg', '.png', '.bmp', '.gif']
        image_files = [f for f in path.glob('*') if f.suffix.lower() in image_extensions]
        
        if not image_files:
            print(f"No image files found in {path}")
            return
        
        print(f"Found {len(image_files)} images to process")
        
        for img_file in image_files:
            print(f"\nProcessing: {img_file}")
            
            if args.cricket:
                # Use cricket-specific functionality
                image_data = app.load_image(str(img_file))
                results = app.process_cricket_specific(image_data)
                
                if results['cricket_data']:
                    print(f"Cricket data detected: Over {results['cricket_data']['over']}, Ball {results['cricket_data']['ball']}")
                    print(f"Confidence: {results['cricket_data']['confidence']}%")
                else:
                    print("No cricket data detected in the image.")
            else:
                # Use general image recognition
                results = app.analyze_image(str(img_file), features=args.features)
                app.print_analysis_summary(results)
                
                if args.output:
                    filename = f"{img_file.stem}_analysis.json"
                    output_path = os.path.join(args.output, filename)
                    app.save_results(results, output_path)
    
    else:
        print(f"Error: {image_path} is not a valid file or directory")


if __name__ == "__main__":
    main()