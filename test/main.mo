import RO "../src/Reorder";
import Nac "mo:nacdb/NacDB";
import M "mo:matchers/Matchers";
import T "mo:matchers/Testable";
import Suite "mo:matchers/Suite";
import Order "mo:base/Order";
import Index "index/main";

let index = await Index.Index();
MyCycles.addPart(dbOptions.partitionCycles);
await index.init();

func prepareOrder(): async* RO.Order {
    RO.createOrder(index: Nac.IndexCanister, guid1: GUID.GUID, guid2: GUID.GUID)
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