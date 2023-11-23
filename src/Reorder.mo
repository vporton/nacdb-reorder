import Nac "mo:nacdb/NacDB";
import OpsQueue "mo:nacdb/OpsQueue";
import GUID "mo:nacdb/GUID";
import Can "mo:candb/CanDB";
import Entity "mo:candb/Entity";
import Prng "mo:prng";
import Nat64 "mo:base/Nat64";
import Buffer "mo:base/Buffer";
import Blob "mo:base/Blob";
import Iter "mo:base/Iter";
import Nat8 "mo:base/Nat8";
import Array "mo:base/Array";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Nat32 "mo:base/Nat32";
import Principal "mo:base/Principal";
import BTree "mo:btree/BTree";

module {
    public type Orderer = {
        var rng: Prng.Seiran128;
        // guidGen: GUID.GUIDGenerator;
        adding: OpsQueue.OpsQueue<AddingItem, ()>;
        block: BTree.BTree<(Nac.OuterCanister, Nac.OuterSubDBKey), ()>;
    };

    public type Order = {
        // A random string is added to a key in order to ensure key are unique.
        order: (Nac.OuterCanister, Nac.OuterSubDBKey); // Key#random -> Value.
        reverse: (Nac.OuterCanister, Nac.OuterSubDBKey); // Value -> Key#random
    };

    type AddingOptions = {
        index: Nac.IndexCanister;
        orderer: Orderer;
        order: Order;
        key: Nac.OuterSubDBKey;
        value: Nat;
    };

    type AddingItem = {
        options: AddingOptions;
        random: Nat;
    };

    /// We assume that all keys have the same length.
    ///
    /// Reentrant.
    public func add(guid: GUID.GUID, options: AddingOptions): async* () {
        ignore OpsQueue.whilePending(options.orderer.adding, func(guid: GUID.GUID, elt: InsertingItem): async* () {
            OpsQueue.answer(
                orderer.adding,
                guid,
                await* addFinishByQueue(guid, elt));
        });

        let random = options.orderer.rng.next(); // should not generate this from GUID, to prevent user favoring his order

        let adding = switch (OpsQueue.get(options.orderer.adding, guid)) {
            case (?op) { { options = op; random } };
            case null {
                // TODO: It is enough to use one condition instead of two, because they are bijective.
                if (BTree.has(options.orderer.block, compareLocs, options.order.order) or
                    BTree.has(options.orderer.block, compareLocs, options.order.reverse)
                ) {
                    Debug.trap("deleting is blocked");
                };
                ignore BTree.insert(options.orderer.block, compareLocs, options.order.order);
                ignore BTree.insert(options.orderer.block, compareLocs, options.order.reverse);
                { options; random };
            };
        };

        try {
            await* addFinishByQueue(guid, adding);
        }
        catch(e) {
            OpsQueue.add(options.orderer.adding, guid, adding);
            throw e;
        };
    };

    public func addFinish(guid: GUID, orderer: Orderer) : async* ?() {
        OpsQueue.result(orderer.adding, guid);
    };

    func addFinishByQueue(guid: GUID, adding: AddingItem) : async* () {
        let key2 = encodeNat(adding.options.key) # encodeNat64(adding.random);
        let q1 = adding.options.index.insert(Blob.toArray(guid), {
            outerCanister = Principal.fromActor(adding.options.order.order.0);
            outerKey = adding.options.order.order.1;
            sk = key2;
            value = #int(adding.options.value);
        });
        let q2 = adding.options.index.insert(Blob.toArray(guid), {
            outerCanister = Principal.fromActor(adding.options.order.reverse.0);
            outerKey = adding.options.order.reverse.1;
            sk = encodeNat(adding.options.value);
            value = #text key2;
        });
        ignore (await q1, await q2); // idempotent

        ignore BTree.delete(options.orderer.block, compareLocs, options.order.order);
        ignore BTree.delete(options.orderer.block, compareLocs, options.order.reverse);
    };

    // TODO: duplicate code with `zondirectory2` repo

    func _toLowerHexDigit(v: Nat): Char {
        Char.fromNat32(Nat32.fromNat(
            if (v < 10) {
                Nat32.toNat(Char.toNat32('0')) + v;
            } else {
                Nat32.toNat(Char.toNat32('a')) + v - 10;
            }
        ));
    };

    func encodeBlob(g: Blob): Text {
        var result = "";
        for (b in g.vals()) {
            let b2 = Nat8.toNat(b);
            result #= Text.fromChar(_toLowerHexDigit(b2 / 16)) # Text.fromChar(_toLowerHexDigit(b2 % 16));
        };
        result;
    };

    public func encodeNat64(n: Nat64): Text {
        var n64 = n;
        let buf = Buffer.Buffer<Nat8>(8);
        for (i in Iter.range(0, 7)) {
        buf.add(Nat8.fromNat(Nat64.toNat(n64 % 256)));
           n64 >>= 8;
        };
        let blob = Blob.fromArray(Array.reverse(Buffer.toArray(buf)));
        encodeBlob(blob);
    };

    public func encodeNat(n: Nat): Text {
        encodeNat64(Nat64.fromNat(n));
    };
}