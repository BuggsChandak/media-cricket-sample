#!/usr/bin/env python3
"""
Example script demonstrating how to use the Image Recognition App.
"""

from Media import ImageRecognitionApp
import argparse
import os

def main():
    """
    Example usage of the Image Recognition App.
    """
    parser = argparse.ArgumentParser(description='Image Recognition App Example')
    parser.add_argument('--image', '-i', required=True, help='Path to the image file')
    args = parser.parse_args()
    
    # Check if the image file exists
    if not os.path.isfile(args.image):
        print(f"Error: Image file '{args.image}' not found.")
        return
    
    print(f"Analyzing image: {args.image}")
    print("=" * 50)
    
    # Initialize the app
    app = ImageRecognitionApp()
    
    # Example 1: Basic analysis with default features (labels and text)
    print("\nExample 1: Basic Analysis (Labels and Text)")
    print("-" * 50)
    results = app.analyze_image(args.image)
    app.print_analysis_summary(results)
    
    # Example 2: Face analysis
    print("\nExample 2: Face Analysis")
    print("-" * 50)
    face_results = app.analyze_image(args.image, features=['faces'])
    app.print_analysis_summary(face_results)
    
    # Example 3: Celebrity recognition
    print("\nExample 3: Celebrity Recognition")
    print("-" * 50)
    celeb_results = app.analyze_image(args.image, features=['celebrities'])
    app.print_analysis_summary(celeb_results)
    
    # Example 4: Content moderation
    print("\nExample 4: Content Moderation")
    print("-" * 50)
    moderation_results = app.analyze_image(args.image, features=['moderation'])
    app.print_analysis_summary(moderation_results)
    
    # Example 5: Comprehensive analysis
    print("\nExample 5: Comprehensive Analysis")
    print("-" * 50)
    comprehensive_results = app.analyze_image(
        args.image, 
        features=['labels', 'faces', 'text', 'celebrities', 'moderation']
    )
    app.print_analysis_summary(comprehensive_results)
    
    # Save the comprehensive results to a file
    output_path = f"{os.path.splitext(args.image)[0]}_analysis.json"
    app.save_results(comprehensive_results, output_path)
    print(f"\nComprehensive analysis results saved to: {output_path}")


if __name__ == "__main__":
    main()