import RO "../src/Reorder";
import Buffer "mo:base/Buffer";
import Nat64 "mo:base/Nat64";
import Nac "mo:nacdb/NacDB";
import M "mo:matchers/Matchers";
import T "mo:matchers/Testable";
import Suite "mo:matchers/Suite";
import Debug "mo:base/Debug";
import Blob "mo:base/Blob";
import Array "mo:base/Array";
import Iter "mo:base/Iter";
import Nat8 "mo:base/Nat8";
import Text "mo:base/Text";
import Int "mo:base/Int";
import Char "mo:base/Char";
import Nat32 "mo:base/Nat32";
import Index "index/main";
import GUID "mo:nacdb/GUID";
import Cycles "mo:base/ExperimentalCycles";

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
        Cycles.add<system>(100_000_000_000);
        let index = await Index.Index();
        await index.init();

        let guidGen = GUID.init(Array.tabulate<Nat8>(16, func _ = 0));

        func createOrder(): async* RO.Order {
            await index.reorderCreateOrder(GUID.nextGuid(guidGen));
        };

        func prepareOrder(): async* RO.Order {
            let order = await* createOrder();
            for (i in Iter.range(0, 2)) {
                await index.reorderAdd(GUID.nextGuid(guidGen), {
                    key = i * (2**32);
                    order;
                    value = encodeInt(i * 10);
                });
            };
            order;
        };

        func moveForwardOrder(): async* RO.Order {
            let order = await* prepareOrder();
            await index.reorderMove(GUID.nextGuid(guidGen), {
                order;
                relative = false;
                newKey = 2 * (2**32) + (2**31);
                value = encodeInt(10);
            });
            order;
        };

        func moveBackwardOrder(): async* RO.Order {
            let order = await* prepareOrder();
            await index.reorderMove(GUID.nextGuid(guidGen), {
                order;
                relative = false;
                newKey = -(2**31);
                value = encodeInt(10);
            });
            order;
        };

        func moveForwardRelativeOrder(): async* RO.Order {
            let order = await* prepareOrder();
            await index.reorderMove(GUID.nextGuid(guidGen), {
                order;
                relative = true;
                newKey = 10 * (2**32) + (2**31);
                value = encodeInt(10);
            });
            order;
        };

        func moveBackwardRelativeOrder(): async* RO.Order {
            let order = await* prepareOrder();
            await index.reorderMove(GUID.nextGuid(guidGen), {
                order;
                relative = true;
                newKey = 10 * -(2**31);
                value = encodeInt(10);
            });
            order;
        };

        let suite = Suite.suite("Reorder test", [
            Suite.suite("Move forward test", do {
                let order1 = await* moveForwardOrder();
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
                        myArray([encodeInt(0), encodeInt(20), encodeInt(10)]))),
                ];
            }),
            Suite.suite("Move backward test", do {
                let order1 = await* moveBackwardOrder();
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
                        myArray([encodeInt(10), encodeInt(0), encodeInt(20)]))),
                ];
            }),
            Suite.suite("Move forward relative test", do {
                let order1 = await* moveForwardRelativeOrder();
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
                        myArray([encodeInt(0), encodeInt(20), encodeInt(10)]))),
                ];
            }),
            Suite.suite("Move backward relative test", do {
                let order1 = await* moveBackwardRelativeOrder();
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
                        myArray([encodeInt(10), encodeInt(0), encodeInt(20)]))),
                ];
            }),
        ]);
        Suite.run(suite);
   };

    func encodeBlob(g: Blob): Text {
        var result = "";
        for (b in g.vals()) {
            let b2 = Nat8.toNat(b);
            result #= Text.fromChar(_toLowerHexDigit(b2 / 16)) # Text.fromChar(_toLowerHexDigit(b2 % 16));
        };
        result;
    };

    func encodeNat64(n: Nat64): Text {
        var n64 = n;
        let buf = Buffer.Buffer<Nat8>(8);
        for (i in Iter.range(0, 7)) {
        buf.add(Nat8.fromNat(Nat64.toNat(n64 % 256)));
           n64 >>= 8;
        };
        let blob = Blob.fromArray(Array.reverse(Buffer.toArray(buf)));
        encodeBlob(blob);
    };

    func encodeNat(n: Nat): Text {
        encodeNat64(Nat64.fromNat(n));
    };

    // For integers less than 2**64 have the same lexigraphical sort order as the argument.
    func encodeInt(n: Int): Text {
        assert n < 2**64;
        if (n >= 0) {
            encodeNat(Int.abs(n));
        } else {
            "-" # encodeNat(2**64 - Int.abs(n));
        };
    };

    func _toLowerHexDigit(v: Nat): Char {
        Char.fromNat32(Nat32.fromNat(
            if (v < 10) {
                Nat32.toNat(Char.toNat32('0')) + v;
            } else {
                Nat32.toNat(Char.toNat32('a')) + v - 10;
            }
        ));
    };
}