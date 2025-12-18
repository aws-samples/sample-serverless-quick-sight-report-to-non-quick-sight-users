# sample-serverless-quicksight-report-to-non-quicksight-users


# Building a Server-less Report Delivery for Non-QuickSight Users

## Overview
Amazon QuickSight Pixel-perfect Reports enables sharing of highly formatted, personalized reports with end-users without infrastructure setup, up-front licensing, or long-term commitments. This solution demonstrates how to deliver QuickSight reports to non-QuickSight users using AWS services in a serverless architecture.

## Use Case
Consider AnyCompany, a fictional SaaS startup providing call center management services. They use QuickSight with anonymous embedding for customer insights and need a solution to generate scheduled or ad-hoc downloadable reports.

## Solution Features
- Submit QuickSight snapshot API requests via AWS API Gateway and Lambda
- Queue request management
- Request deletion capabilities
- Status tracking and listing
- Request status tracking using DynamoDB
- Report generation and S3 delivery using QuickSight snapshot APIs
- Report download via S3 pre-signed URLs

## Prerequisites
- Web interface or mechanism for user inputs and API calls
- AWS CLI with appropriate permissions
- AWS SAM CLI
- Python 3.9+

## Technical Implementation

### 1. Installation Steps

```bash
# Build the application
sam build

# Deploy with default settings (5 concurrent jobs)
sam deploy --guided

# Or deploy with custom concurrent job limits
sam deploy --parameter-overrides MaxConcurrentJobs=10
