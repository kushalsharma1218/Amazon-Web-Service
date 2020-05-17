import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.regions.Regions;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.document.spec.*;

class DynamoDBClient {
	private String tableName = "user-data";
	private AmazonDynamoDB client = null;
	private Table table;
	public DynamoDBClient() throws IOException {
			String accessKey = "#################";
			String secretKey = "#################";
			BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
		    client = AmazonDynamoDBClientBuilder.standard()
			.withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withRegion(Regions.US_EAST_2).build();
			DynamoDB dynamoDB = new DynamoDB(client);
			table = dynamoDB.getTable(tableName);
	}


	public static void logMessage(String msg) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println(sdf.format(new Date()) + " ==> " + msg);
	}

	public String getTableStatus() {
		TableDescription tableDescription = client.describeTable(
				new DescribeTableRequest().withTableName(tableName)).getTable();
		return tableDescription.getTableStatus();
	}

	public Object getPreviousDownload()
	{

		GetItemSpec spec = new GetItemSpec() 
   .withPrimaryKey("email-id", "CS50") 
   .withProjectionExpression("downloads") 
   .withConsistentRead(true);
   Item item = table.getItem(spec);
	return item.get("downloads");
	}

	public void updateTable() {
					List<Map<String,Object>> al=(List<Map<String,Object>>)getPreviousDownload();
				
			

		 
				Map<String, Object> childDownload = new HashMap<String, Object>();
				List<String> tag = new ArrayList<String>();
				childDownload.put("content-type", "jpeg");
				childDownload.put("download", ".jpeg");
				tag.add("TEST5");
				tag.add("TEST5");	
				tag.add("TEST5");
				childDownload.put("tag", tag);
					
					al.add(childDownload);

				 UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("email-id", "CS50")
				 .withUpdateExpression("set downloads = :a")
				 .withValueMap(new ValueMap()
                 .withList(":a", al))
                 .withReturnValues(ReturnValue.UPDATED_NEW);


				  try {
            System.out.println("Updating the item...");
            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
            System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());

        }
        catch (Exception e) {
            System.err.println("Unable to update item: ");
            System.err.println(e.getMessage());
        }
	}
	


	 public void putItems() {
		try { 
		
		 List<Object> downloads = new ArrayList<Object>();
		 Map<String, Object> childDownload = new HashMap<String, Object>();
		 
		List<String> tag = new ArrayList<String>();
        childDownload.put("content-type", "txt");
        childDownload.put("download", "test");
		tag.add("TEST1");
		tag.add("TEST2");
		tag.add("TEST3");
        childDownload.put("tag", tag);
		downloads.add(childDownload);
		table.putItem(new Item() .withPrimaryKey("email-id", "CS50")
				.with("Company","test5").with("First Name", "kushal")
				.with("Industry","test5").with("Last Name","Sharma").with("downloads",downloads));
            System.out.println("Successful load: " );  
         } catch (Exception e) {
            System.out.println("Cannot add product: ");
            System.out.println(e.getMessage()); 
         }
	 }

	
	
	public void listItems() {
		logMessage("List all items");
		ScanRequest scanRequest = new ScanRequest().withTableName(tableName);

		ScanResult result = client.scan(scanRequest);
		for (Map<String, AttributeValue> item : result.getItems()) {
			printItem(item);
		}
	}

	private void printItem(Map<String, AttributeValue> attributeList) {
		String itemString = new String();
		for (Map.Entry<String, AttributeValue> item : attributeList.entrySet()) {
			if (!itemString.equals(""))
				itemString += ", ";
			String attributeName = item.getKey();
			AttributeValue value = item.getValue();
			itemString += attributeName
					+ ""
					+ (value.getS() == null ? "" : "=\"" + value.getS() + "\"");
	}
		logMessage(itemString);
	}

	public static void main(String[] args) {
		try {
			DynamoDBClient dbClient = new DynamoDBClient();

			// dbClient.createTable();
			while (!"ACTIVE".equalsIgnoreCase(dbClient.getTableStatus())) {
				logMessage("Waiting for table being created. Sleeping 10 seconds");
				Thread.sleep(10000);
			}

			// dbClient.putItems();
			dbClient.updateTable();

			while ("UPDATING".equalsIgnoreCase(dbClient.getTableStatus())) {
				logMessage("Waiting for table being updated. Sleeping 10 seconds");
				Thread.sleep(10000);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}