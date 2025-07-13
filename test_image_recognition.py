import unittest
from unittest.mock import patch, MagicMock
import os
import json
from Media import ImageRecognitionApp

class TestImageRecognitionApp(unittest.TestCase):
    """
    Unit tests for the ImageRecognitionApp class.
    """
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a mock for boto3 session and clients
        self.mock_session_patcher = patch('boto3.Session')
        self.mock_session = self.mock_session_patcher.start()
        
        # Mock rekognition client
        self.mock_rekognition = MagicMock()
        self.mock_session.return_value.client.return_value = self.mock_rekognition
        
        # Mock dynamodb resource and table
        self.mock_dynamodb = MagicMock()
        self.mock_table = MagicMock()
        self.mock_dynamodb.Table.return_value = self.mock_table
        self.mock_session.return_value.resource.return_value = self.mock_dynamodb
        
        # Create the app instance
        self.app = ImageRecognitionApp(profile_name='test_profile')
        
        # Create a temporary test image file
        self.test_image_path = '/tmp/test_image.jpg'
        with open(self.test_image_path, 'wb') as f:
            f.write(b'test image data')
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.mock_session_patcher.stop()
        
        # Remove test image file
        if os.path.exists(self.test_image_path):
            os.remove(self.test_image_path)
    
    def test_load_image(self):
        """Test loading an image from a file."""
        image_data = self.app.load_image(self.test_image_path)
        self.assertEqual(image_data, b'test image data')
    
    def test_detect_labels(self):
        """Test detecting labels in an image."""
        # Mock the response from rekognition
        mock_response = {
            'Labels': [
                {'Name': 'Person', 'Confidence': 99.5},
                {'Name': 'Car', 'Confidence': 85.2}
            ]
        }
        self.mock_rekognition.detect_labels.return_value = mock_response
        
        # Call the method
        result = self.app.detect_labels(b'test image data')
        
        # Verify the result
        self.assertEqual(result, mock_response)
        self.mock_rekognition.detect_labels.assert_called_once_with(
            Image={'Bytes': b'test image data'},
            MaxLabels=10,
            MinConfidence=70
        )
    
    def test_detect_faces(self):
        """Test detecting faces in an image."""
        # Mock the response from rekognition
        mock_response = {
            'FaceDetails': [
                {
                    'BoundingBox': {'Width': 0.5, 'Height': 0.5, 'Left': 0.25, 'Top': 0.25},
                    'AgeRange': {'Low': 20, 'High': 30},
                    'Gender': {'Value': 'Male', 'Confidence': 98.5}
                }
            ]
        }
        self.mock_rekognition.detect_faces.return_value = mock_response
        
        # Call the method
        result = self.app.detect_faces(b'test image data')
        
        # Verify the result
        self.assertEqual(result, mock_response)
        self.mock_rekognition.detect_faces.assert_called_once_with(
            Image={'Bytes': b'test image data'},
            Attributes=['ALL']
        )
    
    def test_detect_text(self):
        """Test detecting text in an image."""
        # Mock the response from rekognition
        mock_response = {
            'TextDetections': [
                {
                    'DetectedText': 'Hello World',
                    'Type': 'LINE',
                    'Confidence': 95.2,
                    'Geometry': {'BoundingBox': {'Width': 0.5, 'Height': 0.1, 'Left': 0.25, 'Top': 0.25}}
                }
            ]
        }
        self.mock_rekognition.detect_text.return_value = mock_response
        
        # Call the method
        result = self.app.detect_text(b'test image data')
        
        # Verify the result
        self.assertEqual(result, mock_response)
        self.mock_rekognition.detect_text.assert_called_once_with(
            Image={'Bytes': b'test image data'}
        )
    
    def test_recognize_celebrities(self):
        """Test recognizing celebrities in an image."""
        # Mock the response from rekognition
        mock_response = {
            'CelebrityFaces': [
                {
                    'Name': 'John Doe',
                    'MatchConfidence': 98.7,
                    'Urls': ['https://www.example.com']
                }
            ]
        }
        self.mock_rekognition.recognize_celebrities.return_value = mock_response
        
        # Call the method
        result = self.app.recognize_celebrities(b'test image data')
        
        # Verify the result
        self.assertEqual(result, mock_response)
        self.mock_rekognition.recognize_celebrities.assert_called_once_with(
            Image={'Bytes': b'test image data'}
        )
    
    def test_detect_moderation_labels(self):
        """Test detecting moderation labels in an image."""
        # Mock the response from rekognition
        mock_response = {
            'ModerationLabels': [
                {
                    'Name': 'Violence',
                    'Confidence': 75.3,
                    'ParentName': ''
                }
            ]
        }
        self.mock_rekognition.detect_moderation_labels.return_value = mock_response
        
        # Call the method
        result = self.app.detect_moderation_labels(b'test image data')
        
        # Verify the result
        self.assertEqual(result, mock_response)
        self.mock_rekognition.detect_moderation_labels.assert_called_once_with(
            Image={'Bytes': b'test image data'},
            MinConfidence=50
        )
    
    def test_analyze_image(self):
        """Test analyzing an image with multiple features."""
        # Mock the responses from rekognition
        self.mock_rekognition.detect_labels.return_value = {'Labels': [{'Name': 'Person', 'Confidence': 99.5}]}
        self.mock_rekognition.detect_text.return_value = {'TextDetections': [{'DetectedText': 'Hello', 'Type': 'LINE', 'Confidence': 95.2}]}
        
        # Call the method
        result = self.app.analyze_image(self.test_image_path, features=['labels', 'text'])
        
        # Verify the result
        self.assertEqual(result['labels'], {'Labels': [{'Name': 'Person', 'Confidence': 99.5}]})
        self.assertEqual(result['text'], {'TextDetections': [{'DetectedText': 'Hello', 'Type': 'LINE', 'Confidence': 95.2}]})
        self.assertEqual(result['image_path'], self.test_image_path)
        self.assertTrue('timestamp' in result)
    
    def test_process_cricket_specific(self):
        """Test cricket-specific text detection."""
        # Mock the response from rekognition
        mock_response = {
            'TextDetections': [
                {
                    'DetectedText': '5.2',
                    'Type': 'LINE',
                    'Id': 0,
                    'Confidence': 95.2,
                    'Geometry': {
                        'BoundingBox': {'Width': 0.1, 'Height': 0.05, 'Left': 0.33, 'Top': 0.86}
                    }
                }
            ]
        }
        self.mock_rekognition.detect_text.return_value = mock_response
        
        # Call the method
        result = self.app.process_cricket_specific(b'test image data')
        
        # Verify the result
        self.assertEqual(result['cricket_data']['over'], '5')
        self.assertEqual(result['cricket_data']['ball'], '2')
        self.assertEqual(result['cricket_data']['confidence'], 95.2)
    
    def test_save_to_dynamodb(self):
        """Test saving data to DynamoDB."""
        # Test data
        test_data = {'id': '123', 'name': 'test'}
        
        # Call the method
        self.app.save_to_dynamodb(test_data)
        
        # Verify the result
        self.mock_table.put_item.assert_called_once_with(Item=test_data)


if __name__ == '__main__':
    unittest.main()