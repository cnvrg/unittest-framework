# unittest pipeline:
Run through of unit test pipeline

## Pipeline Trigger :
Currently unit test pipeline is configured to trigger on any push occurs in branch 'main' & 'pipeline' 
and in any pull request events.

## Steps
This pipeline runs on ubuntu image. The key steps are below

1. Checkout code
2. Setup Python Environment
3. Install dependencies
4. Execute unit tests

For the purpose of enabling SSH keyless authentication which is part of unit test execution below steps are included

1. Create temp directory for datasets
2. Initialize ssh keys
   Create key pair and place public, private key as secrets PUB_KEY, PRIV_KEY
   Also have public key to be included in known_hosts as secret KNOWN_HOSTS 
4. Place public key in authorized keys
