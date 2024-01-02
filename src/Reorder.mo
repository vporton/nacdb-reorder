import xNat "mo:xtended-numbers/NatX";
import Nac "mo:nacdb/NacDB";
import OpsQueue "mo:nacdb/OpsQueue";
import GUID "mo:nacdb/GUID";
import Can "mo:candb/CanDB";
import Entity "mo:candb/Entity";
import Nat64 "mo:base/Nat64";
import Buffer "mo:base/Buffer";
import Blob "mo:base/Blob";
import Iter "mo:base/Iter";
import Itertools "mo:itertools/Iter";
import Nat8 "mo:base/Nat8";
import Array "mo:base/Array";
import Text "mo:base/Text";
import Char "mo:base/Char";
import Nat32 "mo:base/Nat32";
import Principal "mo:base/Principal";
import Nat "mo:base/Nat";
import Debug "mo:base/Debug";
import Order "mo:base/Order";
import Int "mo:base/Int";
import Bool "mo:base/Bool";
import Int8 "mo:base/Int8";
import BTree "mo:stableheapbtreemap/BTree";

module {
    public type Orderer = {
        guidGen: GUID.GUIDGenerator;
        adding: OpsQueue.OpsQueue<AddItem, ()>;
        deleting: OpsQueue.OpsQueue<DeleteItem, ()>;
        moving: OpsQueue.OpsQueue<MoveItem, ()>;
        creatingOrder: OpsQueue.OpsQueue<CreateOrderItem, Order>;
        block: BTree.BTree<(Nac.OuterCanister, Nac.OuterSubDBKey), ()>;
    };

    public func createOrderer(): Orderer {
        {
            guidGen = GUID.init(Array.tabulate<Nat8>(16, func _ = 0));
            adding = OpsQueue.init(10); // FIXME: fixed number of entries
            deleting = OpsQueue.init(10);
            moving = OpsQueue.init(10);
            creatingOrder = OpsQueue.init(10);
            block = BTree.init(null);
        };
    };

    /// Keys may be duplicated, but all values are distinct.
    public type Order = {
        // A random string is added to a key in order to ensure key are unique.
        order: (Nac.OuterCanister, Nac.OuterSubDBKey); // Key#random -> Value.
        reverse: (Nac.OuterCanister, Nac.OuterSubDBKey); // Value -> Key#random
    };

    public type AddOptions = {
        order: Order;
        key: Int;
        value: Text;
    };

    public type AddItem = {
        options: AddOptions;
        random: GUID.GUID;
        guid1: GUID.GUID;
        guid2: GUID.GUID;
    };

    /// We assume that all keys have the same length.
    public func add(guid: GUID.GUID, index: Nac.IndexCanister, orderer: Orderer, options: AddOptions): async* () {
        ignore OpsQueue.whilePending(orderer.adding, func(guid: GUID.GUID, elt: AddItem): async* () {
            OpsQueue.answer(
                orderer.adding,
                guid,
                await* addFinishByQueue(guid, index, orderer, elt));
        });

        let adding = switch (OpsQueue.get(orderer.adding, guid)) {
            case (?adding) { adding };
            case null {
                // TODO: It is enough to use one condition instead of two, because they are bijective.
                // TODO: duplicate code
                if (BTree.has(orderer.block, compareLocs, options.order.order) or
                    BTree.has(orderer.block, compareLocs, options.order.reverse)
                ) {
                    Debug.trap("is blocked");
                };
                ignore BTree.insert(orderer.block, compareLocs, options.order.order, ());
                ignore BTree.insert(orderer.block, compareLocs, options.order.reverse, ());

                {
                    options;
                    random = GUID.nextGuid(orderer.guidGen);
                    guid1 = GUID.nextGuid(orderer.guidGen);
                    guid2 = GUID.nextGuid(orderer.guidGen);
                };
            };
        };

        try {
            await* addFinishByQueue(guid, index, orderer, adding);
        }
        catch(e) {
            OpsQueue.add(orderer.adding, guid, adding);
            throw e;
        };
    };

    public func addFinish(guid: GUID.GUID, orderer: Orderer) : async* ?() {
        OpsQueue.result(orderer.adding, guid);
    };

    public func addFinishByQueue(guid: GUID.GUID, index: Nac.IndexCanister, orderer: Orderer, adding: AddItem) : async* () {
        let key2 = encodeInt(adding.options.key) # "#" # encodeBlob(adding.random);
        let q1 = index.insert(Blob.toArray(adding.guid1), {
            outerCanister = Principal.fromActor(adding.options.order.order.0);
            outerKey = adding.options.order.order.1;
            sk = key2;
            value = #text(adding.options.value);
        });
        let q2 = index.insert(Blob.toArray(adding.guid2), {
            outerCanister = Principal.fromActor(adding.options.order.reverse.0);
            outerKey = adding.options.order.reverse.1;
            sk = adding.options.value;
            value = #text key2;
        });
        ignore (await q1, await q2); // idempotent

        ignore BTree.delete(orderer.block, compareLocs, adding.options.order.order);
        ignore BTree.delete(orderer.block, compareLocs, adding.options.order.reverse);
    };

    public type DeleteOptions = {
        order: Order;
        value: Text;
    };

    public type DeleteItem = {
        options: DeleteOptions;
    };

    public func delete(guid: GUID.GUID, index: Nac.IndexCanister, orderer: Orderer, options: DeleteOptions): async* () {
        ignore OpsQueue.whilePending(orderer.deleting, func(guid: GUID.GUID, elt: DeleteItem): async* () {
            OpsQueue.answer(
                orderer.deleting,
                guid,
                await* deleteFinishByQueue(index, orderer, elt));
        });

        let deleting = switch (OpsQueue.get(orderer.deleting, guid)) {
            case (?deleting) { deleting };
            case null {
                // TODO: It is enough to use one condition instead of two, because they are bijective.
                if (BTree.has(orderer.block, compareLocs, options.order.order) or
                    BTree.has(orderer.block, compareLocs, options.order.reverse)
                ) {
                    Debug.trap("is blocked");
                };
                ignore BTree.insert(orderer.block, compareLocs, options.order.order, ());
                ignore BTree.insert(orderer.block, compareLocs, options.order.reverse, ());
                {
                    options;
                    guid1 = GUID.nextGuid(orderer.guidGen);
                    guid2 = GUID.nextGuid(orderer.guidGen);
                };
            };
        };

        try {
            await* deleteFinishByQueue(index, orderer, deleting);
        }
        catch(e) {
            OpsQueue.add(orderer.deleting, guid, deleting);
            throw e;
        };
    };

    public func deleteFinish(guid: GUID.GUID, orderer: Orderer) : async* ?() {
        OpsQueue.result(orderer.deleting, guid);
    };

    public func deleteFinishByQueue(index: Nac.IndexCanister, orderer: Orderer, deleting: DeleteItem) : async* () {
        let key = await deleting.options.order.reverse.0.getByOuter({
            outerKey = deleting.options.order.reverse.1;
            sk = deleting.options.value;
        });

        // The order of two following statements is essential:
        switch (key) {
            case (?#text keyText) {
                await index.delete(Blob.toArray(GUID.nextGuid(orderer.guidGen)), {
                    outerCanister = Principal.fromActor(deleting.options.order.order.0);
                    outerKey = deleting.options.order.order.1;
                    sk = keyText;
                });
            };
            case null {}; // re-execution after an exception
            case _ {
                Debug.trap("programming error");
            }
        };

        await index.delete(Blob.toArray(GUID.nextGuid(orderer.guidGen)), {
            outerCanister = Principal.fromActor(deleting.options.order.reverse.0);
            outerKey = deleting.options.order.reverse.1;
            sk = deleting.options.value;
        });

        ignore BTree.delete(orderer.block, compareLocs, deleting.options.order.order);
        ignore BTree.delete(orderer.block, compareLocs, deleting.options.order.reverse);
    };

    /// Move value to new key.
    public type MoveOptions = {
        order: Order;
        value: Text;
        relative: Bool;
        newKey: Int;
    };

    public type MoveItem = {
        options: MoveOptions;
        random: GUID.GUID;
        guid1: GUID.GUID;
        guid2: GUID.GUID;
        guid3: GUID.GUID;
    };

    public func move(guid: GUID.GUID, index: Nac.IndexCanister, orderer: Orderer, options: MoveOptions): async* () {
        ignore OpsQueue.whilePending(orderer.moving, func(guid: GUID.GUID, elt: MoveItem): async* () {
            OpsQueue.answer(
                orderer.moving,
                guid,
                await* moveFinishByQueue(guid, index, orderer, elt));
        });

        let moving = switch (OpsQueue.get(orderer.moving, guid)) {
            case (?moving) { moving };
            case null {
                // TODO: It is enough to use one condition instead of two, because they are bijective.
                if (BTree.has(orderer.block, compareLocs, options.order.order) or
                    BTree.has(orderer.block, compareLocs, options.order.reverse)
                ) {
                    Debug.trap("is blocked");
                };
                ignore BTree.insert(orderer.block, compareLocs, options.order.order, ());
                ignore BTree.insert(orderer.block, compareLocs, options.order.reverse, ());

                {
                    options;
                    random = GUID.nextGuid(orderer.guidGen);
                    guid1 = GUID.nextGuid(orderer.guidGen);
                    guid2 = GUID.nextGuid(orderer.guidGen);
                    guid3 = GUID.nextGuid(orderer.guidGen);
                };
            };
        };

        try {
            await* moveFinishByQueue(guid, index, orderer, moving);
        }
        catch(e) {
            OpsQueue.add(orderer.moving, guid, moving);
            throw e;
        };
    };

    public func moveFinish(guid: GUID.GUID, orderer: Orderer) : async* ?() {
        OpsQueue.result(orderer.moving, guid);
    };

    public func moveFinishByQueue(guid: GUID.GUID, index: Nac.IndexCanister, orderer: Orderer, moving: MoveItem) : async* () {
        let newValueText = moving.options.value;
        let oldKey = await moving.options.order.reverse.0.getByOuter({ // FIXME: Shouldn't it be `getByOuter`? (here and in other places)
            outerKey = moving.options.order.reverse.1;
            sk = newValueText;
        });
        let newKey = switch (oldKey) {
            case (?#text oldKeyText) {
                let oldKeyMainPart = Text.fromIter(Itertools.takeWhile(oldKeyText.chars(), func(c: Char): Bool { c != '#' }));
                // TODO: Apparently superfluous decodeInt/encodeInt pair
                let newKey = if (moving.options.relative) {
                    decodeInt(oldKeyMainPart) + moving.options.newKey;
                } else {
                    moving.options.newKey;
                };
                if (encodeInt(newKey) == oldKeyMainPart) {
                    return;
                };
                newKey;
            };
            case _ {
                Debug.trap("no reorder key"); // FIXME: Here and in other places, unblock on trap.
            };
        };
        let newKeyText = encodeInt(newKey) # "#" # encodeBlob(moving.random);

        let q1 = index.insert(Blob.toArray(moving.guid1), {
            outerCanister = Principal.fromActor(moving.options.order.order.0);
            outerKey = moving.options.order.order.1;
            sk = newKeyText;
            value = #text(moving.options.value);
        });
        let q2 = index.insert(Blob.toArray(moving.guid2), {
            outerCanister = Principal.fromActor(moving.options.order.reverse.0);
            outerKey = moving.options.order.reverse.1;
            sk = newValueText;
            value = #text(newKeyText);
        });
        ignore (await q1, await q2); // idempotent
        switch (oldKey) {
            case (?#text oldKeyText) {
                await index.delete(Blob.toArray(moving.guid3), {
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

        ignore BTree.delete(orderer.block, compareLocs, moving.options.order.order);
        ignore BTree.delete(orderer.block, compareLocs, moving.options.order.reverse);
    };

    public type CreateOrderItem = {
        guid1: GUID.GUID;
        guid2: GUID.GUID;
        order: ?(Principal, Nac.OuterSubDBKey); // To increase performace, store `OuterCanister` instead.
    };

    public func createOrder(guid: GUID.GUID, index: Nac.IndexCanister, orderer: Orderer): async* Order {
        ignore OpsQueue.whilePending(orderer.creatingOrder, func(guid: GUID.GUID, elt: CreateOrderItem): async* () {
            OpsQueue.answer(
                orderer.creatingOrder,
                guid,
                await* createOrderFinishByQueue(guid, index, orderer, elt));
        });

        let creatingOrder = switch (OpsQueue.get(orderer.creatingOrder, guid)) {
            case (?moving) { moving };
            case null {
                {
                    guid1 = GUID.nextGuid(orderer.guidGen);
                    guid2 = GUID.nextGuid(orderer.guidGen);
                    order = null;
                };
            };
        };

        try {
            await* createOrderFinishByQueue(guid, index, orderer, creatingOrder);
        }
        catch(e) {
            OpsQueue.add(orderer.creatingOrder, guid, creatingOrder);
            throw e;
        };
    };

    public func createOrderFinish(guid: GUID.GUID, orderer: Orderer) : async* ?Order {
        OpsQueue.result(orderer.creatingOrder, guid);
    };

    // I run promises in order, rather than paralelly, to ensure they are executed once.
    public func createOrderFinishByQueue(guid: GUID.GUID, index: Nac.IndexCanister, orderer: Orderer, creatingOrder: CreateOrderItem) : async* Order {
        let order = switch(creatingOrder.order) {
            case (?order) { order };
            case null {
                (await index.createSubDB(Blob.toArray(creatingOrder.guid1), {userData = ""})).outer;
            }
        };
        let reverse = (await index.createSubDB(Blob.toArray(creatingOrder.guid2), {userData = ""})).outer;
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

    func _fromLowerHexDigit(c: Char): Nat {
        Nat32.toNat(
        if (c <= '9') {
            Char.toNat32(c) - Char.toNat32('0');
        } else {
            Char.toNat32(c) - Char.toNat32('a') + 10;
        }
        );
    };

    func decodeBlob(t: Text): Blob {
        let buf = Buffer.Buffer<Nat8>(t.size() / 2);
        let c = t.chars();
        label r loop {
        let ?upper = c.next() else {
            break r;
        };
        let ?lower = c.next() else {
            Debug.trap("decodeBlob: wrong hex number");
        };
        let b = Nat8.fromNat(_fromLowerHexDigit(upper) * 16 + _fromLowerHexDigit(lower));
        buf.add(b);
        };
        Blob.fromArray(Buffer.toArray(buf));
    };

    func decodeNat(t: Text): Nat {
        let blob = decodeBlob(t);
        var result: Nat64 = 0;
        for (b in blob.vals()) {
            result <<= 8;
            result += xNat.from8To64(b);
        };
        Nat64.toNat(result);
    };

    func decodeInt(t: Text): Int {
        let iter = t.chars();
        if (iter.next() == ?'-') {
            -(2**64 - decodeNat(Text.fromIter(iter)));
        } else {
            decodeNat(t);
        }
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