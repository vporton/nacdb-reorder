import RO "../Reorder";
import M "mo:matchers/Matchers";
import T "mo:matchers/Testable";
import Suite "mo:matchers/Suite";

func prepareOrder() {
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