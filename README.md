## SQL Repository - NCLCA CCO Team

This repository is a central location for SQL scripts to enable source control and change tracking for .sql files.

## Usage
SQL Scripts should be saved in subdirectories within the Scripts folder. This repository blocks all files not saved in a subdirectory within the Scripts folder from appearing on the remote repository.

Tables maintained by dedicated pipelines in other repositories do not need to be stored here to prevent multiple versions of scripts and allow for python maintained tables. Instead add a text file (.txt) with a link to the relevant repository and a brief explanation of what tables are maintained by that pipeline.

### Best Practice

Ensure all scripts follow the [NCL Naming Convention documentation](https://github.com/ncl-icb-analytics/snowflake-public/wiki/NCL-Snowflake-naming-conventions). 

When adding or editing files, it is best practice to create a new branch before making changes and merging the new branch back into the main branch when finished. This allows for multiple people to work on the same file simutaneously and leaves an unaltered set of scripts available until all changes are final. There is an oppertunity to have new scripts peer-reviewed in the dedicated branch before merging the changes back to the main branch.

For files added to the repository, add a comment to the top of the file with a description of what the script does as well the main contact responsible for the script.

Save the "DEV" version of scripts to the repository. You can save a seperate file as the Production version of the script but ensure changes are developed on scripts pointing to DEV tables to prevent unintentional changes being made to Production tables.

#### In summary:

* Develop your scripts in Snowflake locally until you are ready to store them centrally
* Ensure you add a comment to the top of your script containing a description and details of the main person responsible for the script overall
* Create a new branch ([Guide](https://code.visualstudio.com/docs/sourcecontrol/overview#_branches-and-tags))
* For new scripts:
  * If required, create a new subdirectory in the Scripts folder for your scripts
  * Add your new script files to the relevant subdirectory
* For edits to existing scripts:
  * Replace the existing script files
  * You made need to edit the comment and contacts at the top of the file
* Commit changes made on the new branch ([Guide](https://code.visualstudio.com/docs/sourcecontrol/overview#_commit))  
*Make sure you use "Publish Branch" after making your commits so others can see your new branch (CTRL+Shift+P -> "Git:Publish Branch...")*
* Perform a peer-review on your new scripts by sharing details of the code to review and the branch containing the changes to a colleague ([Details on Peer Reviews](https://github.com/ncl-icb-analytics/snowflake-public/wiki/Peer-Review-Process))

## Licence
This repository is dual licensed under the [Open Government v3]([https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) & MIT. All code can outputs are subject to Crown Copyright.

## Contact
Jake Kealey - jake.kealey@nhs.net