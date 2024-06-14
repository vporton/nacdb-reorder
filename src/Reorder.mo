import xNat "mo:xtended-numbers/NatX";
import Nac "mo:nacdb/NacDB";
import OpsQueue "mo:nacdb/OpsQueue";
import GUID "mo:nacdb/GUID";
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
import BTree "mo:stableheapbtreemap/BTree";

module {
    /// `Orderer` is used to manage `Order` objects.
    /// You should have one `Orderer` per application (e.g. as a `stable` variable).
    ///
    /// Threat this as an opaque type.
    public type Orderer = {
        guidGen: GUID.GUIDGenerator;
        adding: OpsQueue.OpsQueue<AddItem, ()>;
        deleting: OpsQueue.OpsQueue<DeleteItem, ()>;
        moving: OpsQueue.OpsQueue<MoveItem, Bool>;
        creatingOrder: OpsQueue.OpsQueue<CreateOrderItem, Order>;
        block: BTree.BTree<(Nac.OuterCanister, Nac.OuterSubDBKey), ()>;
    };

    /// Create an `Orderer`. Queue length (see `nacdb` package, `OpsQueue` module) are specified.
    public func createOrderer({queueLengths: Nat}): Orderer {
        {
            guidGen = GUID.init(Array.tabulate<Nat8>(16, func _ = 0));
            adding = OpsQueue.init(queueLengths);
            deleting = OpsQueue.init(queueLengths);
            moving = OpsQueue.init(queueLengths);
            creatingOrder = OpsQueue.init(queueLengths);
            block = BTree.init(null);
        };
    };

    /// `Order` represent a list of distinct values ordered by string keys:
    ///
    /// * `order` is a NacDB sub-DB: Key#random -> Value
    /// * `reverse` is a NacDB sub-DB: Value -> Key#random
    ///
    /// To create an `Order` create two NacDB sub-DBs and assign it to these fields.
    ///
    /// Keys may be duplicated, but all values must be distinct.
    public type Order = {
        // A random string is added to a key in order to ensure key are unique.
        order: (Nac.OuterCanister, Nac.OuterSubDBKey); // Key#random -> Value.
        reverse: (Nac.OuterCanister, Nac.OuterSubDBKey); // Value -> Key#random
    };

    public type AddOptions = {
        index: Nac.IndexCanister;
        dbIndex: Nac.DBIndex;
        orderer: Orderer;
        order: Order;
        key: Int;
        value: Text;
        hardCap: ?Nat;
    };

    public type AddItem = {
        options: AddOptions;
        random: GUID.GUID;
        guid1: GUID.GUID;
        guid2: GUID.GUID;
    };

    /// Add a key-value pair to an `Order`. The key is inserted as it should by the order.
    ///
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

                {
                    options;
                    random = GUID.nextGuid(options.orderer.guidGen);
                    guid1 = GUID.nextGuid(options.orderer.guidGen);
                    guid2 = GUID.nextGuid(options.orderer.guidGen);
                };
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

    /// Finish an interrupted `add` task.
    public func addFinish(guid: GUID.GUID, orderer: Orderer) : async* ?() {
        OpsQueue.result(orderer.adding, guid);
    };

    public func addFinishByQueue(_guid: GUID.GUID, adding: AddItem) : async* () {
        let key2 = encodeInt(adding.options.key) # "#" # encodeBlob(adding.random);
        ignore await* Nac.insert(adding.guid1, {
            indexCanister = Principal.fromActor(adding.options.index);
            dbIndex = adding.options.dbIndex;
            outerCanister = Principal.fromActor(adding.options.order.order.0);
            outerKey = adding.options.order.order.1;
            sk = key2;
            value = #text(adding.options.value);
            hardCap = adding.options.hardCap;
        });
        ignore await* Nac.insert(adding.guid2, {
            indexCanister = Principal.fromActor(adding.options.index);
            dbIndex = adding.options.dbIndex;
            outerCanister = Principal.fromActor(adding.options.order.reverse.0);
            outerKey = adding.options.order.reverse.1;
            sk = adding.options.value;
            value = #text key2;
            hardCap = adding.options.hardCap;
        });

        ignore BTree.delete(adding.options.orderer.block, compareLocs, adding.options.order.order);
        ignore BTree.delete(adding.options.orderer.block, compareLocs, adding.options.order.reverse);
    };

    public type DeleteOptions = {
        index: Nac.IndexCanister;
        dbIndex: Nac.DBIndex;
        orderer: Orderer;
        order: Order;
        value: Text;
    };

    public type DeleteItem = {
        options: DeleteOptions;
        guid1: GUID.GUID;
        guid2: GUID.GUID;
    };

    /// Delete a key/value pair (modifies both NacDB sub-DBs `order` and `reverse`).
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
                {
                    options;
                    guid1 = GUID.nextGuid(options.orderer.guidGen);
                    guid2 = GUID.nextGuid(options.orderer.guidGen);
                };
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

    /// Finish an interrupted `delete` operation.
    public func deleteFinish(guid: GUID.GUID, orderer: Orderer) : async* ?() {
        OpsQueue.result(orderer.deleting, guid);
    };

    public func deleteFinishByQueue(_guid: GUID.GUID, deleting: DeleteItem) : async* () {
        let key = await deleting.options.order.reverse.0.getByOuter({
            outerKey = deleting.options.order.reverse.1;
            sk = deleting.options.value;
        });

        // The order of two following statements is essential:
        switch (key) {
            case (?#text keyText) {
                await* Nac.delete(deleting.guid1, {
                    dbIndex = deleting.options.dbIndex;
                    outerCanister = deleting.options.order.order.0;
                    outerKey = deleting.options.order.order.1;
                    sk = keyText;
                });
            };
            case null {}; // re-execution after an exception
            case _ {
                ignore BTree.delete(deleting.options.orderer.block, compareLocs, deleting.options.order.order);
                ignore BTree.delete(deleting.options.orderer.block, compareLocs, deleting.options.order.reverse);
                Debug.trap("programming error");
            }
        };

        await* Nac.delete(deleting.guid2, {
            dbIndex = deleting.options.dbIndex;
            outerCanister = deleting.options.order.reverse.0;
            outerKey = deleting.options.order.reverse.1;
            sk = deleting.options.value;
        });

        ignore BTree.delete(deleting.options.orderer.block, compareLocs, deleting.options.order.order);
        ignore BTree.delete(deleting.options.orderer.block, compareLocs, deleting.options.order.reverse);
    };

    /// Move value to new key.
    public type MoveOptions = {
        index: Nac.IndexCanister;
        dbIndex: Nac.DBIndex;
        orderer: Orderer;
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

    /// Move an item in `order` (that is in both two Nac sub-DBs) to a position specified by `newKey`.
    /// If `relative`, then `newKey` is added to an existing order value rathen than replace it.
    ///
    /// Returns `true` if there has been an old value or `false` if inserted.
    public func move(guid: GUID.GUID, options: MoveOptions): async* Bool {
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

                {
                    options;
                    random = GUID.nextGuid(options.orderer.guidGen);
                    guid1 = GUID.nextGuid(options.orderer.guidGen);
                    guid2 = GUID.nextGuid(options.orderer.guidGen);
                    guid3 = GUID.nextGuid(options.orderer.guidGen);
                };
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

    /// Finish an interrupted move operation.
    public func moveFinish(guid: GUID.GUID, orderer: Orderer) : async* ?Bool {
        OpsQueue.result(orderer.moving, guid);
    };

    public func moveFinishByQueue(_guid: GUID.GUID, moving: MoveItem) : async* Bool {
        let newValueText = moving.options.value;
        let oldKey = await moving.options.order.reverse.0.getByOuter({
            outerKey = moving.options.order.reverse.1;
            sk = newValueText;
        });
        let hadOld = oldKey != null;
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
                    ignore BTree.delete(moving.options.orderer.block, compareLocs, moving.options.order.order);
                    ignore BTree.delete(moving.options.orderer.block, compareLocs, moving.options.order.reverse);
                    return hadOld;
                };
                newKey;
            };
            case _ {
                moving.options.newKey;
                // ignore BTree.delete(orderer.block, compareLocs, moving.options.order.order);
                // ignore BTree.delete(orderer.block, compareLocs, moving.options.order.reverse);
                // Debug.trap("no reorder key");
            };
        };
        let newKeyText = encodeInt(newKey) # "#" # encodeBlob(moving.random);

        ignore await* Nac.insert(moving.guid1, {
            indexCanister = Principal.fromActor(moving.options.index);
            dbIndex = moving.options.dbIndex;
            outerCanister = Principal.fromActor(moving.options.order.order.0);
            outerKey = moving.options.order.order.1;
            sk = newKeyText;
            value = #text(moving.options.value);
            hardCap = null;
        });
        ignore await* Nac.insert(moving.guid2, {
            indexCanister = Principal.fromActor(moving.options.index);
            dbIndex = moving.options.dbIndex;
            outerCanister = Principal.fromActor(moving.options.order.reverse.0);
            outerKey = moving.options.order.reverse.1;
            sk = newValueText;
            value = #text(newKeyText);
            hardCap = null;
        });
        switch (oldKey) {
            case (?#text oldKeyText) {
                await* Nac.delete(moving.guid3, {
                    dbIndex = moving.options.dbIndex;
                    outerCanister = moving.options.order.order.0;
                    outerKey = moving.options.order.order.1;
                    sk = oldKeyText;
                });
            };
            case null {}; // re-execution after an exception
            case _ {
                ignore BTree.delete(moving.options.orderer.block, compareLocs, moving.options.order.order);
                ignore BTree.delete(moving.options.orderer.block, compareLocs, moving.options.order.reverse);
                Debug.trap("programming error");
            }
        };

        ignore BTree.delete(moving.options.orderer.block, compareLocs, moving.options.order.order);
        ignore BTree.delete(moving.options.orderer.block, compareLocs, moving.options.order.reverse);

        hadOld;
    };

    public type CreateOrderOptions = {
        index: Nac.IndexCanister;
        dbIndex: Nac.DBIndex;
        orderer: Orderer;
        hardCap: ?Nat;
    };

    public type CreateOrderItem = {
        options: CreateOrderOptions;
        guid1: GUID.GUID;
        guid2: GUID.GUID;
        order: ?{canister: Nac.OuterCanister; key: Nac.OuterSubDBKey}; // TODO: To increase performace, store `OuterCanister` instead.
    };

    /// Create an `Order` (two NacDB sub-DBs).
    public func createOrder(guid: GUID.GUID, options: CreateOrderOptions): async* Order {
        ignore OpsQueue.whilePending(options.orderer.creatingOrder, func(guid: GUID.GUID, elt: CreateOrderItem): async* () {
            OpsQueue.answer(
                options.orderer.creatingOrder,
                guid,
                await* createOrderFinishByQueue(guid, elt));
        });

        let creatingOrder = switch (OpsQueue.get(options.orderer.creatingOrder, guid)) {
            case (?moving) { moving };
            case null {
                {
                    options;
                    guid1 = GUID.nextGuid(options.orderer.guidGen);
                    guid2 = GUID.nextGuid(options.orderer.guidGen);
                    order = null;
                };
            };
        };

        try {
            await* createOrderFinishByQueue(guid, creatingOrder);
        }
        catch(e) {
            OpsQueue.add(options.orderer.creatingOrder, guid, creatingOrder);
            throw e;
        };
    };

    /// Finish an interrupted `createOrder` operation.
    public func createOrderFinish(guid: GUID.GUID, orderer: Orderer) : async* ?Order {
        OpsQueue.result(orderer.creatingOrder, guid);
    };

    // I run promises in order, rather than paralelly, to ensure they are executed once.
    public func createOrderFinishByQueue(_guid: GUID.GUID, creatingOrder: CreateOrderItem) : async* Order {
        let order = switch(creatingOrder.order) {
            case (?order) { order };
            case null {
                (await* Nac.createSubDB(creatingOrder.guid1, {
                    index = creatingOrder.options.index;
                    dbIndex = creatingOrder.options.dbIndex;
                    userData = "";
                    hardCap = creatingOrder.options.hardCap;
                })).outer;
            }
        };
        let reverse = (await* Nac.createSubDB(creatingOrder.guid2, {
            index = creatingOrder.options.index;
            dbIndex = creatingOrder.options.dbIndex;
            userData = "";
            hardCap = creatingOrder.options.hardCap;
        })).outer;
        {
            order = (order.canister, order.key);
            reverse = (reverse.canister, reverse.key);
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