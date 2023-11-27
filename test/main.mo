import RO "../src/Reorder";
import Nac "mo:nacdb/NacDB";
import M "mo:matchers/Matchers";
import T "mo:matchers/Testable";
import Suite "mo:matchers/Suite";
import Order "mo:base/Order";
import Index "index/main";
import MyCycles "mo:nacdb/Cycles";
import GUID "mo:nacdb/GUID";
import Common "common";

let index = await Index.Index();
MyCycles.addPart(Common.dbOptions.partitionCycles);
await index.init();

func prepareOrder(orderer: RO.Orderer): async* RO.Order {
    RO.createOrder(guid, {orderer})
};

func main() {
    let suite = Suite.suite("Reorder test", [
        Suite.suite("Nat tests", [
            Suite.test("10 is 10", 10, M.equals(T.nat(10))),
            Suite.test("5 is greater than three", 5, M.greaterThan<Nat>(3)),
        ])
    ]);
    Suite.run(suite);
};