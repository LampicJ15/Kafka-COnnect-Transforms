# Kafka Connect [SMT 4 NeoCDC]
**Kafka single message transforms that handle the Neo4j CDC format**

This is a collection of Single Message Transforms for Kafka Connect modified to work best with the Neo4j CDC format. So far it contains:
* FilterTransform

### FilterTransform

This transformer filters the incoming CDC records from Neo4j.
It checks if the record is about a node or a relationship change and then compares the defined record fields values 
with the given labels. It passes the record if the given field contains the given labels, otherwise it filters it (returns a `null`).

#### Coniguration and parameters
First you have to add the FilterTransformer **JAR** to the *KAFKA CONNECT* plugin path (*/usr/share/kafka/plugins*).

Name | Description | Type | Default value | Importance
---- | ----------- | ---- | ------------ | ------------
node.field | The field containing the value on which to check the equality to perform message filtering. | String | payload.after.labels | Low
node.second.field | The secondary node field containing the labels on which to check the equality to perform message filtering if the primary node field is empty. | String |payload.before.labels| Low
labels | The value to compare to the node field value (to check the equality condition). | String List|/| High
relationship.start.field | The name of the relationship field that represents the start node.| String | payload.start.labels| Low
relationship.end.field | The name of the relationship field that represents the end node. | String | payload.end.labels | Low

#### Examples

##### i.) Node CDC record example
Lets consider the following example. We receive multiple Node CDC records from Kafka.
Lets say that we only want to receive *node records* that contain the label *Person* (like the record below).
```
{
  "meta": {
    "timestamp": 1532597182604,
    "username": "neo4j",
    "tx_id": 3,
    "tx_event_id": 0,
    "tx_events_count": 2,
    "operation": "created",
    "source": {
      "hostname": "neo4j.mycompany.com"
    }
  },
  "payload": {
    "id": "1004",
    "type": "node",
    "after": {
      "labels": ["Person"],
      "properties": {
        "email": "annek@noanswer.org",
        "last_name": "Kretchmar",
        "first_name": "Anne Marie"
      }
    }
  },
  "schema": {
    "properties": {
      "last_name": "String",
      "email": "String",
      "first_name": "String"
    },
    "constraints": [{
      "label": "Person",
      "properties": ["first_name", "last_name"],
      "type": "UNIQUE"
    }]
  }
}
```
When the record is about a **created** or **updated** node, the filter checks the field `payload.after.labels`
and checks if the field contains the desired label (in our case label *Person*). 
If the record is about a **deleted** node, the filter checks the field `payload.before.labels`.

In that case your connector config would look like (showing only transforms part):
```
"name": "my-connector",
  "config": {
	"transforms": "filter, nullHandler",		
	"transforms.filter.type": "si.hekovnik.transform.FilterTransform$Value",
	"transforms.filter.labels": ["Person"]
	"transforms.nullHandler.type":"io.confluent.connect.transforms.TombstoneHandler",
	"transforms.nullHandler.behavior": "ignore"
  }
} 
```
**WARNNING: You have to add the *TombstoneHandler* so that Neo4j can handle `null` records.**

##### ii.) Relationship CDC record example
Consider the following example. We receive multiple Relationship CDC records from Kafka.
Lets say that we only want to receive *relationship records* where the relationships connect nodes 
with the given labels. Lets say that we only want relationships that start in *Person* node and end in a *Person*
node (:Person)-->(:Person). 
```
{
  "meta": {
    "timestamp": 1532597182604,
    "username": "neo4j",
    "tx_id": 3,
    "tx_event_id": 0,
    "tx_events_count": 2,
    "operation": "created",
    "source": {
      "hostname": "neo4j.mycompany.com"
    }
  },
  "payload": {
    "id": "123",
    "type": "relationship",
    "label": "KNOWS",
    "start": {
      "labels": ["Person"],
      "id": "123",
      "ids": {
        "last_name": "Andrea",
        "first_name": "Santurbano"
      }
    },
    "end": {
      "labels": ["Person"],
      "id": "456",
      "ids": {
        "last_name": "Michael",
        "first_name": "Hunger"
      }
    },
    "after": {
      "properties": {
        "since": "2018-04-05T12:34:00[Europe/Berlin]"
      }
    }
  },
  "schema": {
    "properties": {
      "since": "ZonedDateTime"
    },
    "constraints": [{
      "label": "KNOWS",
      "properties": ["since"],
      "type": "RELATIONSHIP_PROPERTY_EXISTS"
    }]
  }
}
```
When the record is about a relationship, the filter checks the field `payload.start.labels` and
`payload.end.labels` and checks if the fields contain the desired labels (in our case label *Person*). 

In that case your connector config would look like (showing only transforms part):
```
"name": "my-connector",
  "config": {
	"transforms": "filter, nullHandler",		
	"transforms.filter.type": "si.hekovnik.transform.FilterTransform$Value",
	"transforms.filter.labels": ["Person"]
	"transforms.nullHandler.type":"io.confluent.connect.transforms.TombstoneHandler",
	"transforms.nullHandler.behavior": "ignore"
  }
} 
```
**WARNNING: You have to add the *TombstoneHandler* so that Neo4j can handle `null` records.**

**WARNING: Field defining is case sensitive.**

##TODO:
+ Filter for records with schema.
+ Add more complex conditions like "<", "<=",...