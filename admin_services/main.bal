import ballerina/log;
import ballerina/http;
import ballerina/kafka;

// HTTP listener
listener http:Listener listener = new(9096);

// Kafka producer to publish schedule updates
kafka:Producer eventProducer = new({
    bootstrapServers: "localhost:9092",
    topic: "schedule.updates"
});

// Mock data storage for routes, trips, and ticket reports
json[] routes = [];
json[] trips = [];
json[] ticketReports = [];

service /admin on listener {

    // Add a new route
    resource function post route(http:Caller caller, http:Request req) returns error? {
        json payload = check req.getJsonPayload();
        payload["id"] = routes.length() + 1;
        routes.push(payload);
        check caller->respond({ status: "Route added", route: payload });
    }

    // Add a new trip
    resource function post trip(http:Caller caller, http:Request req) returns error? {
        json payload = check req.getJsonPayload();
        payload["id"] = trips.length() + 1;
        trips.push(payload);

        // Publish to Kafka (trip schedule update)
        kafka:ProducerRecord rec = { value: payload };
        var result = eventProducer->send(rec);
        if (result is error) {
            log:printError("Failed to send Kafka event", result);
        }

        check caller->respond({ status: "Trip added", trip: payload });
    }

    // View all routes
    resource function get routes(http:Caller caller, http:Request req) returns error? {
        check caller->respond({ routes: routes });
    }

    // View all trips
    resource function get trips(http:Caller caller, http:Request req) returns error? {
        check caller->respond({ trips: trips });
    }

    // Mock ticket sales report
    resource function get reports(http:Caller caller, http:Request req) returns error? {
        check caller->respond({ reports: ticketReports });
    }

    // Publish a service disruption or schedule change
    resource function post disruption(http:Caller caller, http:Request req) returns error? {
        json payload = check req.getJsonPayload();

        // Publish disruption message via Kafka
        kafka:ProducerRecord rec = { value: payload };
        var result = eventProducer->send(rec);
        if (result is error) {
            log:printError("Failed to send Kafka event", result);
        }

        check caller->respond({ status: "Disruption announced", disruption: payload });
    }
}
