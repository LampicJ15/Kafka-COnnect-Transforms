# kafka-connect-transform

## Single message transforms for Kafka Connect

This is a collection of Single Message Transforms for Kafka Connect. So far it contains:
* FilterTransform

### FilterTransform

This transformer will check if the given *field* contains the given *fieldValue*. If the condition is **false** the kafka record message is filtered, otherwise nothing changes. 

#### Coniguration and parameters
You have to add the FilterTransformer **JAR** to the *KAFKA CONNECT* plugin path (*/usr/share/kafka/plugins*).

Name | Description | Type | Valid values | Importance
---- | ----------- | ---- | ------------ | ------------
field | The field containing the value on which to check the equality to perform message filtering. | String |  | High
field.values | The value to compare to the field value (to check the equality condition). | String List| | High
second.field | The secondary field containing the value on which to check the equality to perform message filtering (if the primary field is empty). | String |  | Low

#### Examples

Lets consider the following dataset of a group of people:

```
{"firstName": "FirstName1", "lastName": "LastName1", "age": 30}
{"firstName": "FirstName2", "lastName": "LastName2", "age": 30}
{"firstName": "FirstName3", "lastName": "LastName1", "age": 60}
{"firstName": "FirstName4", "lastName": "LastName2", "age": 60}
```

We want to to filter all those records whose *lastName* is *LastName2*. In that case your connector config would look like (showing only transforms part):
```
"name": "my-connector",
  "config": {
	"transforms": "filter",		
	"transforms.filter.type": "si.hekovnik.transform.FilterTransform$Value",
	"transforms.filter.field": "lastName",
	"transforms.filter.field.values": ["LastName2"]
  }
} 
```
If we want to filter a nested field we separate the fields with ".".
For example, lets say that we want to filter the field *age*, we set the filtered field as *Person.age* .
```
{"Person": {"firstName": "FirstName1", "lastName": "LastName1", "age": 30}}
```

**WARNING: Field defining is case sensitive.**

##TODO:
+ Filter for records with schema.
+ Add more complex conditions like "<", "<=",...