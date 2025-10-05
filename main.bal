import ballerinax/mysql;
import ballerina/http;
import ballerina/log;
import ballerina/kafka;

endpoint http:Listener listener {
    port: 9092
};

// Kafka producer for schedule updates
kafka:Producer scheduleProducer = new({
    bootstrapServers: "localhost:9092",
    topic: "schedule.updates"
});

// MySQL client
mysql:Client dbClient = new ({
    host: "localhost",
    port: 3306,
    username: "root",
    password: "Amen2005_k",
    database: "ticketing_db"
});

service /transport on listener {

    // Create a new route
    resource function post route(http:Caller caller, http:Request req) returns error? {
        json payload = check req.getJsonPayload();
        string name = <string>payload.name;
        string origin = <string>payload.origin;
        string destination = <string>payload.destination;

        var result = dbClient->execute(
            "INSERT INTO routes(name, origin, destination) VALUES (?, ?, ?)",
            name, origin, destination
        );

        if (result is error) {
            log:printError("Failed to insert route", result);
            check caller->respond("Route creation failed");
            return;
        }
        check caller->respond("Route created successfully");
    }

    // Create a new trip
    resource function post trip(http:Caller caller, http:Request req) returns error? {
        json payload = check req.getJsonPayload();
        int routeId = <int>payload.route_id;
        string departure = <string>payload.departure_time;
        string arrival = <string>payload.arrival_time;
        int capacity = <int>payload.capacity;

        var result = dbClient->execute(
            "INSERT INTO trips(route_id, departure_time, arrival_time, capacity) VALUES (?, ?, ?, ?)",
            routeId, departure, arrival, capacity
        );

        if (result is error) {
            log:printError("Failed to insert trip", result);
            check caller->respond("Trip creation failed");
            return;
        }

        // Publish Kafka event
        var kafkaResult = scheduleProducer->send({
            value: "New trip created for route " + routeId.toString()
        });

        if (kafkaResult is error) {
            log:printError("Failed to send Kafka message", kafkaResult);
        }

        check caller->respond("Trip created successfully");
    }

    // Get all routes
    resource function get routes(http:Caller caller, http:Request req) returns error? {
        stream<record {int id; string name; string origin; string destination;}, mysql:Error> rows =
            dbClient->query("SELECT id, name, origin, destination FROM routes");

        json routeList = [];
        check from r in rows do { routeList.push(r); };

        check caller->respond(routeList);
    }

    // Get all trips
    resource function get trips(http:Caller caller, http:Request req) returns error? {
        stream<record {int id; int route_id; string departure_time; string arrival_time; int capacity;}, mysql:Error> rows =
            dbClient->query("SELECT id, route_id, departure_time, arrival_time, capacity FROM trips");

        json tripList = [];
        check from t in rows do { tripList.push(t); };

        check caller->respond(tripList);
    }
}
