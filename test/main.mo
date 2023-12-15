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
    let myArrayTestable : T.Testable<[Text]> = {
        display = func(a : [Text]) : Text = debug_show(a);
        equals = func(n1 : [Text], n2 : [Text]) : Bool = n1 == n2;
    };

    func myArray(n : [Text]) : T.TestableItem<[Text]> = {
        item = n;
        display = myArrayTestable.display;
        equals = myArrayTestable.equals;
    };

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
                    key = i * (2**32);
                    order;
                    orderer;
                    value = Reorder.encodeInt(i * 10);
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
                newKey = 2 * (2**32) + (2**31);
                value = Reorder.encodeInt(10);
            });
            order;
        };

        func moveBackwardOrder(orderer: RO.Orderer): async* RO.Order {
            let order = await* prepareOrder(orderer);
            await* Reorder.move(GUID.nextGuid(guidGen), {
                index;
                order;
                orderer;
                newKey = -(2**31);
                value = Reorder.encodeInt(10);
            });
            order;
        };

        let suite = Suite.suite("Reorder test", [
            Suite.suite("Move forward test", do {
                let order1 = await* moveForwardOrder(orderer);
                let results = await order1.order.0.scanLimitOuter({
                    outerKey = order1.order.1;
                    lowerBound = "";
                    upperBound = "zz";
                    dir = #fwd;
                    limit = 1000;
                });
                let results2 = Iter.map<(Text, Nac.AttributeValue), Text>(Array.vals(results.results), func((k, v): (Text, Nac.AttributeValue)) {
                    let #text v2 = v else {
                        Debug.trap("programming error");
                    };
                    v2;
                });
                [
                    Suite.test("move element forward", Iter.toArray(results2), M.equals(
                        myArray([Reorder.encodeInt(0), Reorder.encodeInt(20), Reorder.encodeInt(10)]))),
                ];
            }),
            Suite.suite("Move backward test", do {
                let order1 = await* moveBackwardOrder(orderer);
                let results = await order1.order.0.scanLimitOuter({
                    outerKey = order1.order.1;
                    lowerBound = "";
                    upperBound = "zz";
                    dir = #fwd;
                    limit = 1000;
                });
                let results2 = Iter.map<(Text, Nac.AttributeValue), Text>(Array.vals(results.results), func((k, v): (Text, Nac.AttributeValue)) {
                    let #text v2 = v else {
                        Debug.trap("programming error");
                    };
                    v2;
                });
                [
                    Suite.test("move element forward", Iter.toArray(results2), M.equals(
                        myArray([Reorder.encodeInt(10), Reorder.encodeInt(0), Reorder.encodeInt(20)]))),
                ];
            }),
        ]);
        Suite.run(suite);
   };
}