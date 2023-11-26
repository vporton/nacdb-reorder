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
import Nat "mo:base/Nat";
import Debug "mo:base/Debug";
import BTree "mo:btree/BTree";

module {
    public type Orderer = {
        var rng: Prng.Seiran128; // 64 bits seems enough (https://stackoverflow.com/a/22029380/856090)
        // guidGen: GUID.GUIDGenerator;
        adding: OpsQueue.OpsQueue<AddItem, ()>;
        deleting: OpsQueue.OpsQueue<DeleteItem, ()>;
        moving: OpsQueue.OpsQueue<MoveItem, ()>;
        block: BTree.BTree<(Nac.OuterCanister, Nac.OuterSubDBKey), ()>;
    };

    /// Keys may be duplicated, but all values are distinct.
    public type Order = {
        // A random string is added to a key in order to ensure key are unique.
        order: (Nac.OuterCanister, Nac.OuterSubDBKey); // Key#random -> Value.
        reverse: (Nac.OuterCanister, Nac.OuterSubDBKey); // Value -> Key#random
    };

    // FIXME: Below I use the same GUID more than once. That's an error.

    public type AddOptions = {
        index: Nac.IndexCanister;
        orderer: Orderer;
        order: Order;
        key: Nac.OuterSubDBKey;
        value: Nat;
    };

    public type AddItem = {
        options: AddOptions;
        random: Nat64;
    };

    /// We assume that all keys have the same length.
    public func add(guid: GUID.GUID, options: AddOptions): async* () {
        ignore OpsQueue.whilePending(options.orderer.adding, func(guid: GUID.GUID, elt: AddItem): async* () {
            OpsQueue.answer(
                options.orderer.adding,
                guid,
                await* addFinishByQueue(guid, elt));
        });

        let adding = switch (OpsQueue.get(options.orderer.adding, guid)) {
            case (?adding) { adding };
            case null {
                // TODO: It is enough to use one condition instead of two, because they are bijective.
                // TODO: duplicate code
                if (BTree.has(options.orderer.block, compareLocs, options.order.order) or
                    BTree.has(options.orderer.block, compareLocs, options.order.reverse)
                ) {
                    Debug.trap("is blocked");
                };
                ignore BTree.insert(options.orderer.block, compareLocs, options.order.order, ());
                ignore BTree.insert(options.orderer.block, compareLocs, options.order.reverse, ());

                let random = options.orderer.rng.next(); // should not generate this from GUID, to prevent user favoring his order
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

    public func addFinish(guid: GUID.GUID, orderer: Orderer) : async* ?() {
        OpsQueue.result(orderer.adding, guid);
    };

    public func addFinishByQueue(guid: GUID.GUID, adding: AddItem) : async* () {
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

        ignore BTree.delete(adding.options.orderer.block, compareLocs, adding.options.order.order);
        ignore BTree.delete(adding.options.orderer.block, compareLocs, adding.options.order.reverse);
    };

    public type DeleteOptions = {
        index: Nac.IndexCanister;
        orderer: Orderer;
        order: Order;
        value: Nat;
    };

    public type DeleteItem = {
        options: DeleteOptions;
        // random: Nat64;
    };

    public func delete(guid: GUID.GUID, options: DeleteOptions): async* () {
        ignore OpsQueue.whilePending(options.orderer.deleting, func(guid: GUID.GUID, elt: DeleteItem): async* () {
            OpsQueue.answer(
                options.orderer.deleting,
                guid,
                await* deleteFinishByQueue(guid, elt));
        });

        let deleting = switch (OpsQueue.get(options.orderer.deleting, guid)) {
            case (?deleting) { deleting };
            case null {
                // TODO: It is enough to use one condition instead of two, because they are bijective.
                if (BTree.has(options.orderer.block, compareLocs, options.order.order) or
                    BTree.has(options.orderer.block, compareLocs, options.order.reverse)
                ) {
                    Debug.trap("is blocked");
                };
                ignore BTree.insert(options.orderer.block, compareLocs, options.order.order, ());
                ignore BTree.insert(options.orderer.block, compareLocs, options.order.reverse, ());
                { options };
            };
        };

        try {
            await* deleteFinishByQueue(guid, deleting);
        }
        catch(e) {
            OpsQueue.add(options.orderer.deleting, guid, deleting);
            throw e;
        };
    };

    public func deleteFinish(guid: GUID.GUID, orderer: Orderer) : async* ?() {
        OpsQueue.result(orderer.deleting, guid);
    };

    public func deleteFinishByQueue(guid: GUID.GUID, deleting: DeleteItem) : async* () {
        let key = await deleting.options.order.reverse.0.getByInner({
            innerKey = deleting.options.order.reverse.1;
            sk = encodeNat(deleting.options.value);
        });

        // The order of two following statements is essential:
        switch (key) {
            case (?#text keyText) {
                await deleting.options.order.order.0.deleteInner({
                    innerKey = deleting.options.order.order.1;
                    sk = keyText;
                });
            };
            case null {}; // re-execution after an exception
            case _ {
                Debug.trap("programming error");
            }
        };

        await deleting.options.order.reverse.0.deleteInner({
            innerKey = deleting.options.order.reverse.1;
            sk = encodeNat(deleting.options.value);
        });

        ignore BTree.delete(deleting.options.orderer.block, compareLocs, deleting.options.order.order);
        ignore BTree.delete(deleting.options.orderer.block, compareLocs, deleting.options.order.reverse);
    };

    /// Move value to new key.
    public type MoveOptions = {
        index: Nac.IndexCanister;
        orderer: Orderer;
        order: Order;
        newKey: Nac.OuterSubDBKey;
        value: Nat;
    };

    public type MoveItem = {
        options: MoveOptions;
        random: Nat64;
    };

    public func move(guid: GUID.GUID, options: MoveOptions): async* () {
        ignore OpsQueue.whilePending(options.orderer.moving, func(guid: GUID.GUID, elt: MoveItem): async* () {
            OpsQueue.answer(
                options.orderer.moving,
                guid,
                await* moveFinishByQueue(guid, elt));
        });

        let moving = switch (OpsQueue.get(options.orderer.moving, guid)) {
            case (?moving) { moving };
            case null {
                // TODO: It is enough to use one condition instead of two, because they are bijective.
                if (BTree.has(options.orderer.block, compareLocs, options.order.order) or
                    BTree.has(options.orderer.block, compareLocs, options.order.reverse)
                ) {
                    Debug.trap("is blocked");
                };
                ignore BTree.insert(options.orderer.block, compareLocs, options.order.order, ());
                ignore BTree.insert(options.orderer.block, compareLocs, options.order.reverse, ());

                let random = options.orderer.rng.next(); // should not generate this from GUID, to prevent user favoring his order
                { options; random };
            };
        };

        try {
            await* moveFinishByQueue(guid, moving);
        }
        catch(e) {
            OpsQueue.add(options.orderer.moving, guid, moving);
            throw e;
        };
    };

    public func moveFinish(guid: GUID.GUID, orderer: Orderer) : async* ?() {
        OpsQueue.result(orderer.moving, guid);
    };

    public func moveFinishByQueue(guid: GUID.GUID, moving: MoveItem) : async* () {
        let newValueText = encodeNat(moving.options.value);
        let oldKey = await moving.options.order.reverse.0.getByInner({
            innerKey = moving.options.order.reverse.1;
            sk = newValueText;
        });
        if (?#int(moving.options.newKey) == oldKey) {
            return;
        };
        let newKeyText = encodeNat(moving.options.newKey);

        let q1 = moving.options.index.insert(Blob.toArray(guid), {
            outerCanister = Principal.fromActor(moving.options.order.order.0);
            outerKey = moving.options.order.order.1;
            sk = newKeyText;
            value = #int(moving.options.value);
        });
        let q2 = moving.options.index.insert(Blob.toArray(guid), {
            outerCanister = Principal.fromActor(moving.options.order.reverse.0);
            outerKey = moving.options.order.reverse.1;
            sk = newValueText;
            value = #text(newKeyText);
        });
        ignore (await q1, await q2); // idempotent
        switch (oldKey) {
            case (?#text oldKeyText) {
                await moving.options.index.delete(Blob.toArray(guid), {
                    outerCanister = Principal.fromActor(moving.options.order.order.0);
                    outerKey = moving.options.order.order.1;
                    sk = oldKeyText;
                });
            };
            case null {}; // re-execution after an exception
            case _ {
                Debug.trap("programming error");
            }
        };

        ignore BTree.delete(moving.options.orderer.block, compareLocs, moving.options.order.order);
        ignore BTree.delete(moving.options.orderer.block, compareLocs, moving.options.order.reverse);
    };

    public func createOrder(index: Nac.IndexCanister, guid1: GUID.GUID, guid2: GUID.GUID): async* Order {
        let order = (await index.createSubDB(Blob.toArray(guid1), {userData = ""})).outer;
        let reverse = (await index.createSubDB(Blob.toArray(guid2), {userData = ""})).outer;
        {
            order = (actor(Principal.toText(order.0)), order.1);
            reverse = (actor(Principal.toText(reverse.0)), reverse.1);
        };
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

    func comparePartition(x: Nac.PartitionCanister, y: Nac.PartitionCanister): {#equal; #greater; #less} {
        Principal.compare(Principal.fromActor(x), Principal.fromActor(y));
    };

    func compareLocs(x: (Nac.PartitionCanister, Nac.SubDBKey), y: (Nac.PartitionCanister, Nac.SubDBKey)): {#equal; #greater; #less} {
        let c = comparePartition(x.0, y.0);
        if (c != #equal) {
            c;
        } else {
            Nat.compare(x.1, y.1);
        }
    };
}