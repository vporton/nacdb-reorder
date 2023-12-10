import RO "../src/Reorder";
import Nac "mo:nacdb/NacDB";
import M "mo:matchers/Matchers";
import T "mo:matchers/Testable";
import Suite "mo:matchers/Suite";
import Order "mo:base/Order";
import Debug "mo:base/Debug";
import Blob "mo:base/Blob";
import Array "mo:base/Array";
import Iter "mo:base/Iter";
import Index "index/main";
import MyCycles "mo:nacdb/Cycles";
import GUID "mo:nacdb/GUID";
import Common "common";
import Reorder "../src/Reorder";

actor Test {
    public func main(): async () {
        MyCycles.addPart(Common.dbOptions.partitionCycles);
        let index = await Index.Index();
        MyCycles.addPart(Common.dbOptions.partitionCycles);
        await index.init();

        let orderer = RO.createOrderer(index);

        func createOrder(orderer: RO.Orderer): async* RO.Order {
            await* RO.createOrder(GUID.nextGuid(orderer.guidGen), {orderer});
        };

        let guidGen = GUID.init(Array.tabulate<Nat8>(16, func _ = 0));

        func prepareOrder(orderer: RO.Orderer): async* RO.Order {
            let order = await* createOrder(orderer);
            for (i in Iter.range(0, 2)) {
                await* Reorder.add(GUID.nextGuid(guidGen), {
                    index;
                    key = i * (2**64);
                    order;
                    orderer;
                    value = i * 10;
                });
            };
            order;
        };

        func moveForwardOrder(orderer: RO.Orderer): async* RO.Order {
            let order = await* prepareOrder(orderer);
            await* Reorder.move(GUID.nextGuid(guidGen), {
                index;
                order;
                orderer;
                newKey = 3 * (2**64);
                value = 10; // FIXME
            });
            order;
        };

        let suite = Suite.suite("Reorder test", [
            Suite.suite("Nat tests", [
                Suite.test("10 is 10", 10, M.equals(T.nat(11))),
                Suite.test("5 is greater than three", 5, M.greaterThan<Nat>(3)),
            ])
        ]);
        Suite.run(suite);
   };
}