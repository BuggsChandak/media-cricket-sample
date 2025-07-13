# Image Recognition App

A versatile image recognition application that uses AWS Rekognition to analyze images for various features including objects, scenes, faces, text, celebrities, and content moderation.

## Features

- **Object and Scene Detection**: Identify objects, scenes, and activities in images
- **Face Analysis**: Detect faces and analyze attributes like age range, gender, emotions, etc.
- **Text Detection**: Extract text from images
- **Celebrity Recognition**: Identify celebrities in images
- **Content Moderation**: Detect unsafe or inappropriate content
- **Cricket Match Analysis**: Legacy feature for detecting cricket match information

## Requirements

- Python 3.6+
- AWS account with Rekognition access
- Boto3 library
- AWS credentials configured

## Installation

1. Clone this repository
2. Configure your AWS credentials (using AWS CLI or environment variables)
3. Install required packages:
   ```
   pip install boto3
   ```

## Usage

### Command Line Interface

```bash
# Basic usage with default features (labels and text)
python Media.py --image path/to/image.jpg

# Specify features to detect
python Media.py --image path/to/image.jpg --features labels faces text

# Process all images in a directory
python Media.py --image path/to/directory

# Save results to a specific location
python Media.py --image path/to/image.jpg --output results.json

# Use a specific AWS profile
python Media.py --image path/to/image.jpg --profile my-profile

# Use cricket-specific analysis (legacy feature)
python Media.py --image path/to/image.jpg --cricket
```

### Python API

```python
from Media import ImageRecognitionApp

# Initialize the app
app = ImageRecognitionApp()

# Analyze an image with specific features
results = app.analyze_image('path/to/image.jpg', 
                           features=['labels', 'faces', 'text'])

# Print a summary of the results
app.print_analysis_summary(results)

# Save results to a file
app.save_results(results, 'analysis_results.json')
```

## Testing

Run the unit tests:

```bash
python -m unittest test_image_recognition.py
```

## Legacy Cricket Match Analysis

This app maintains backward compatibility with the original cricket match analysis functionality. To use this feature:

```bash
python Media.py --image path/to/cricket_image.jpg --cricket
```

This will detect cricket match information (over and ball numbers) at specific positions in the image and can store the results in DynamoDB.