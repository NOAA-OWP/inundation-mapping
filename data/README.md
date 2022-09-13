## Source Data Folder

This folder is for python and other files to communicate with external data sources.

The first implementation is for communication to AWS (Amazon Web Service) and has a sub-folder to match.

The first AWS api implementation in the form of talking to AWS S3 buckets has also been adeed and a sub-folder to match.

Generally the pattern will be expected as:

- `src/data`
   -  `{a root data source name}`: Such as AWS, or possibily NWM, NHD, 3Dep or whatever.
       - `A service class`: A python file for any basic API or service you are communication with. In this case there is one for S3. Any communication for S3 buckets will be in this file, including get, pull, get bucket lists, etc. If other AWS api is added, you would generally have an other python file. For example, if we start communicating interactively with ECR or Lambda, there is an argument for it being a new python file.
       - `A parent class`: In the case of AWS, one thing all aws interactions have some things in commons, such as the need to authenticate. By having a base class, all AWS child (inherited) classes automatically have that code available. Using inheritance helps keep standardization of how we communicate with AWS. Helper or common utility classes can also be added if required. One example that has been added is that many AWS communication need an boto3 client object. A one line call by child classes mean the child class has a boto3 client object if required.  Later, if we need a boto3 resource object, that can be added as well. Note: Broad utilities methods that are greater than just AWS, will likely be added at the higher folders (such src/utils or src/data/).

The new `s3.py` is constucted so it can be used as a CLI tool (input param args), but any function can be called directly. You will notice that a starting system of passing in a parameter for `action_type` has been added. At this time, only the option of `put` is available. If a need arises later to pull data from S3, the existing command line parameters will not need to change. Only minor adjustements to the __ main __ and add new methods for "get".  If another python file calls directly over to this python file, it will not go through the __ main __ method but straight to the "get" or "put" or whatever.
