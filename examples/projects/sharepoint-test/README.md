# Sample Pathway program for SharePoint connection testing

Check out the [Pathway SharePoint connector docs](https://pathway.com/developers/api-docs/pathway-xpacks-sharepoint/#pathway.xpacks.connectors.sharepoint.read) and [getting SharePoint secrets tutorial](https://www.youtube.com/watch?v=9ks6zhAPAz4) for more information.

**Usage**:
1. Replace the placeholders in `sharepoint_test.py` (`url`, `root_path`, `tenant`, `client_id`, `thumbprint`, `cert_path`) with actual values.
2. Run the program: `python sharepoint_test.py`
3. If you have a license key, you can provide it as an environment variable: `PATHWAY_LICENSE_KEY=... python sharepoint_test.py`
4. If `contents.txt` contains information after running the program, then the connection was successful.

**Note**: Ensure that your SharePoint space contains at least one file for this test to work.
