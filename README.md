# Cat Sizer

Based on @chinh-tran work, License under MIT (C) @chinh-tran @tawalaya 2021

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install all required packages.

```bash
pip install -r requirements.txt
```

## Usage

Usage:`python sizier.py <stepfunction-arn> <stepfunction.json> <elat_constraint> <payloads.json> <sizes...>`
 - elat_constraint: maximum elat that is allowed for the complete workflow. default is 2000 ms
 - payloads.json: a json that maps lambda arns to payloads that are send when sampling each function.
 - sizes: list of possible sizes to sample. default is [128,256,512,1024,2048,3096]

**warning this will always perfrom sampling for each arn in the stepfunction.json thus create costs**