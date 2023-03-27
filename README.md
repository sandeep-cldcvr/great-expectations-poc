# great-expectations-poc
Great Expectations POC to generate and validate expectations with onboarding data assistant

### 1) How do I get set up? ###
* Run `python3 -m venv venvs/great-expectations-poc && source venvs/great-expectations-poc/bin/activate`
* Run  `pip install --upgrade pip && pip install -r requirements.txt`
* Run  `aws sso login` to authenticate into aws account

### 2) How to create Expectations Suite for an Athena table ?
- Run `python create_configure_datasource.py`
This will create an expectations suite in `great_expectations/expectations` folder in json format
This will also run the checkpoint on the table data and store the validation result in `great_expectations/uncommitted/validations` folder in json format
