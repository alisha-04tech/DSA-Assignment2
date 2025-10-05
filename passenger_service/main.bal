import ballerinax/mysql;
import ballerina/http;
import ballerina/log;

endpoint http:Listener listener {
    port: 9090
};

// MySQL client configuration
mysql:Client dbClient = check new ({
    host: "localhost",
    port: 3306,
    name: "ticketing_db",
    username: "root",
    password: "Amen2005_k"
});

// GET /passengers - List all passengers
service /passengers on listener {

    resource function get .() returns http:Response|error {
        json result;
        sql:Result sqlResult = check dbClient->query("SELECT id, name, email, created_at FROM passengers");
        
        result = [];

        // Convert SQL result to JSON
        foreach row in sqlResult {
            result.push({
                id: row["id"].toString(),
                name: row["name"].toString(),
                email: row["email"].toString(),
                created_at: row["created_at"].toString()
            });
        }

        http:Response res = new;
        res.setJsonPayload(result);
        return res;
    }

    // POST /passengers - Add a new passenger
    resource function post .(http:Caller caller, http:Request req) returns error? {
        json payload = check req.getJsonPayload();
        string name = payload.name.toString();
        string email = payload.email.toString();
        string password = payload.password.toString();

        // Insert passenger into DB
        _ = check dbClient->execute(
            "INSERT INTO passengers(name,email,password) VALUES(?,?,?)",
            name, email, password
        );

        http:Response res = new;
        res.setTextPayload("Passenger created successfully!");
        check caller->respond(res);
    }
}
