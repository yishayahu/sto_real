#ifndef DB_HOT_OINDEX_H_
#define DB_HOT_OINDEX_H_
#pragma once

#include "DB_index.hh"
#include "hot/rowex/HOTRowex.hpp"
#include "hot/rowex/HOTRowexIterator.hpp"
#include "idx/contenthelpers/PairPointerKeyExtractor.hpp"
#include <utility>

namespace bench {
    template <typename K, typename V, typename DBParams>
    class hot_ordered_index : public TObject {
    public:
        typedef K key_type;
        typedef V value_type;
        typedef commutators::Commutator<value_type> comm_type;

        //typedef typename get_occ_version<DBParams>::type occ_version_type;
        typedef typename get_version<DBParams>::type version_type;
        static constexpr typename version_type::type invalid_bit = TransactionTid::user_bit;
        static constexpr TransItem::flags_type insert_bit = TransItem::user0_bit;
        static constexpr TransItem::flags_type delete_bit = TransItem::user0_bit << 1u;
        static constexpr TransItem::flags_type row_update_bit = TransItem::user0_bit << 2u;
        static constexpr TransItem::flags_type row_cell_bit = TransItem::user0_bit << 3u;
        // TicToc node version bit
        static constexpr uintptr_t ttnv_bit = 1 << 1u;

        typedef typename value_type::NamedColumn NamedColumn;
        typedef IndexValueContainer<V, version_type> value_container_type;

        static constexpr bool value_is_small = is_small<V>::value;

        static constexpr bool index_read_my_write = DBParams::RdMyWr;

        struct internal_elem {
            key_type key;
            value_container_type row_container;
            bool deleted;

            internal_elem(const key_type& k, const value_type& v, bool valid)
                    : key(k),
                      row_container((valid ? Sto::initialized_tid() : (Sto::initialized_tid() | invalid_bit)),
                                    !valid, v),
                      deleted(false) {}

            version_type& version() {
                return row_container.row_version();
            }
            key_type first(){
                return key;
            }
            value_type second(){
                return row_container;
            }
            bool valid() {
                return !(version().value() & invalid_bit);
            }
        };
        static void thread_init() {

        }

        using column_access_t = typename split_version_helpers<hot_ordered_index<K, V, DBParams>>::column_access_t;
        using item_key_t = typename split_version_helpers<hot_ordered_index<K, V, DBParams>>::item_key_t;
        template <typename T>
        static constexpr auto column_to_cell_accesses
                = split_version_helpers<hot_ordered_index<K, V, DBParams>>::template column_to_cell_accesses<T>;
        template <typename T>
        static constexpr auto extract_item_list
                = split_version_helpers<hot_ordered_index<K, V, DBParams>>::template extract_item_list<T>;

        typedef std::tuple<bool, bool, uintptr_t, const value_type*> sel_return_type;
        typedef std::tuple<bool, bool>                               ins_return_type;
        typedef std::tuple<bool, bool>                               del_return_type;
        typedef std::tuple<bool, bool, uintptr_t, UniRecordAccessor<V>> sel_split_return_type;

        hot_ordered_index(size_t init_size) {
            key_gen_ = 0;
            (void)init_size;
        }
        hot_ordered_index() {
            key_gen_ = 0;
        }


        uint64_t gen_key() {
            return fetch_and_add(&key_gen_, 1);

        }

        sel_return_type
        select_row(const key_type& key, RowAccess access) {
            auto row_iter = table_.find(key);
            if (row_iter != table_.end()) {
                internal_elem* row = *row_iter;
                bool ok = true;
                TransProxy row_item = Sto::item(this, item_key_t::row_item_key(row));
                if (is_phantom(row, row_item)){
                    if (row->deleted){
                        return sel_return_type(true, false, 0, nullptr);
                    }
                    else {
                        goto abort;
                    }
                }
                if (index_read_my_write) {
                    if (has_delete(row_item)) {
                        assert(false); //we should not get here
                        return sel_return_type(true, false, 0, nullptr);
                    }
                    if (has_row_update(row_item)) {
                        value_type *vptr;
                        if (has_insert(row_item))
                            vptr = &row->row_container.row;
                        else
                            vptr = row_item.template raw_write_value<value_type*>();
                        return sel_return_type(true, true, reinterpret_cast<uintptr_t>((*row_iter)), vptr);
                    }
                }
                switch (access) {
                    case RowAccess::UpdateValue:
                        ok = version_adapter::select_for_update(row_item, row->version());
                        row_item.add_flags(row_update_bit);
                        break;
                    case RowAccess::ObserveExists:
                    case RowAccess::ObserveValue:
                        ok = row_item.observe(row->version());
                        break;
                    default:
                        break;
                }

                if (!ok){
                    goto abort;
                }

                return sel_return_type(true, true,reinterpret_cast<uintptr_t>((*row_iter)), &(row->row_container.row));
            } else
            {
                return {true, false, 0, nullptr};
            }
            abort:
                return sel_return_type(false, false, 0, nullptr);
        }

        void update_row(uintptr_t rid, value_type *new_row) {
            auto e = reinterpret_cast<internal_elem*>(rid);
            auto row_item = Sto::item(this, item_key_t::row_item_key(e));
            if (value_is_small) {
                row_item.acquire_write(e->version(), *new_row);
            } else {
                row_item.acquire_write(e->version(), new_row);
            }
        }

        void update_row(uintptr_t rid, const comm_type &comm) {
            assert(false); //should never get here
            assert(&comm);
            auto e = reinterpret_cast<hot::rowex::HOTRowexSynchronizedIterator<value_type,idx::contenthelpers::PairPointerKeyExtractor>>(rid);
            auto row_item = Sto::item(this, item_key_t::row_item_key(e));
            row_item.add_commute(comm);
        }


        ins_return_type
        insert_row(const key_type& key, value_type *vptr, bool overwrite = false) {
            auto row = table_.find(key);
            if (row != table_.end() && !((*row)->deleted)) {

                TransProxy row_item = Sto::item(this, item_key_t::row_item_key(*row));
                if (is_phantom(*row, row_item)){

                    goto abort;
                }
                if (index_read_my_write) {

                    if (has_delete(row_item)) {
                        assert(false);// should never get here
                        auto proxy = row_item.clear_flags(delete_bit).clear_write();
                        if (value_is_small)
                            proxy.add_write(*vptr);
                        else
                            proxy.add_write(vptr);

                        return ins_return_type(true, false);
                    }
                }
                if (overwrite) {
                    bool ok;

                    if (value_is_small){

                        ok = version_adapter::select_for_overwrite(row_item, (*row)->version(), *vptr);
                    }
                    else
                        ok = version_adapter::select_for_overwrite(row_item,(*row)->version(), vptr);
                    if (!ok){

                        goto abort;
                    }
                    if (index_read_my_write) {

                        if (has_insert(row_item)) {
                            copy_row(*row, vptr);
                        }
                    }
                } else {
                    // observes that the row exists, but nothing more
                    if (!row_item.observe((*row)->version()))
                        goto abort;
                }
                return ins_return_type(true, true);
            } else {
                internal_elem* e;
                if(row != table_.end()){
                    e = *row;
                    e->deleted = false;
                    e->row_container = value_container_type((Sto::initialized_tid() | invalid_bit),
                                                     true, vptr ? *vptr : value_type());
                }
                else{
                    e = new internal_elem(key, vptr ? *vptr : value_type(), false /*!valid*/);
                    table_.insert(e);
                }


                fence();
                TransProxy row_item = Sto::item(this, item_key_t::row_item_key(e));
                row_item.acquire_write(e->version());
                row_item.add_flags(insert_bit);
                return ins_return_type(true, false);
            }
            abort:
                return ins_return_type(false, false);
        }


        void nontrans_put(const key_type& k, const value_type& v) {
            auto e = new internal_elem(k, v, true);
            table_.insert(e);
        }

        // TObject interface methods
        bool lock(TransItem& item, Transaction &txn) override {
            auto key = item.key<item_key_t>();
            auto e = key.internal_elem_ptr();
            if (key.is_row_item())
                return txn.try_lock(item, e->version());
            else
                return txn.try_lock(item, e->row_container.version_at(key.cell_num()));
        }

        bool check(TransItem& item, Transaction& txn) override {
            auto key = item.key<item_key_t>();
            auto e = key.internal_elem_ptr();
            if (key.is_row_item())
                return e->version().cp_check_version(txn, item);
            else
                return e->row_container.version_at(key.cell_num()).cp_check_version(txn, item);
        }

        void install(TransItem& item, Transaction& txn) override {
            auto key = item.key<item_key_t>();
            auto e = key.internal_elem_ptr();

            if (key.is_row_item()) {

                if (!has_insert(item)) {
                    if (item.has_commute()) {
                        comm_type &comm = item.write_value<comm_type>();
                        if (has_row_update(item)) {
                            copy_row(e, comm);
                        } else if (has_row_cell(item)) {
                            e->row_container.install_cell(comm);
                        }
                    } else {
                        value_type *vptr;
                        if (value_is_small) {
                            vptr = &(item.write_value<value_type>());
                        } else {
                            vptr = item.write_value<value_type *>();
                        }

                        if (has_row_update(item)) {
                            if (value_is_small) {
                                e->row_container.row = *vptr;
                            } else {
                                copy_row(e, vptr);
                            }
                        } else if (has_row_cell(item)) {
                            // install only the difference part
                            // not sure if works when there are more than 1 minor version fields
                            // should still work
                            e->row_container.install_cell(0, vptr);
                        }
                    }
                }
                txn.set_version_unlock(e->version(), item);
                assert(e->valid());
            } else {
                assert(false); //should never get here
                // skip installation if row-level update is present
                auto row_item = Sto::item(this, item_key_t::row_item_key(e));
                if (!has_row_update(row_item)) {
                    if (row_item.has_commute()) {
                        comm_type &comm = row_item.template write_value<comm_type>();
                        assert(&comm);
                        e->row_container.install_cell(comm);
                    } else {
                        value_type *vptr;
                        if (value_is_small)
                            vptr = &(row_item.template raw_write_value<value_type>());
                        else
                            vptr = row_item.template raw_write_value<value_type *>();

                        e->row_container.install_cell(key.cell_num(), vptr);
                    }
                }

                txn.set_version_unlock(e->row_container.version_at(key.cell_num()), item);
            }
        }

        void unlock(TransItem& item) override {
            auto key = item.key<item_key_t>();
            auto e = key.internal_elem_ptr();
            if (key.is_row_item())
                e->version().cp_unlock(item);
            else
                e->row_container.version_at(key.cell_num()).cp_unlock(item);
        }

        void cleanup(TransItem& item, bool committed) override {
            if (committed ? has_delete(item) : has_insert(item)) {
                auto key = item.key<item_key_t>();
                assert(key.is_row_item());
                internal_elem *e = key.internal_elem_ptr();
                bool ok = _remove(e->key);
                if (!ok) {
                    std::cout << "committed=" << committed << ", "
                              << "has_delete=" << has_delete(item) << ", "
                              << "has_insert=" << has_insert(item) << ", "
                              << "locked_at_commit=" << item.locked_at_commit() << std::endl;
                    always_assert(false, "insert-bit exclusive ownership violated");
                }
                item.clear_needs_unlock();
            }
        }



    private:
        hot::rowex::HOTRowex<internal_elem*, idx::contenthelpers::PairPointerKeyExtractor>  table_;
        uint64_t key_gen_;

        static bool
        access_all(std::array<access_t, value_container_type::num_versions>& cell_accesses, std::array<TransItem*,
                value_container_type::num_versions>& cell_items, value_container_type& row_container) {
            for (size_t idx = 0; idx < cell_accesses.size(); ++idx) {
                auto& access = cell_accesses[idx];
                auto proxy = TransProxy(*Sto::transaction(), *cell_items[idx]);
                if (static_cast<uint8_t>(access) & static_cast<uint8_t>(access_t::read)) {
                    if (!proxy.observe(row_container.version_at(idx)))
                        return false;
                }
                if (static_cast<uint8_t>(access) & static_cast<uint8_t>(access_t::write)) {
                    if (!proxy.acquire_write(row_container.version_at(idx)))
                        return false;
                    if (proxy.item().key<item_key_t>().is_row_item()) {
                        proxy.item().add_flags(row_cell_bit);
                    }
                }
            }
            return true;
        }

        static bool has_insert(const TransItem& item) {
            return (item.flags() & insert_bit) != 0;
        }
        static bool has_delete(const TransItem& item) {
            return (item.flags() & delete_bit) != 0;
        }
        static bool has_row_update(const TransItem& item) {
            return (item.flags() & row_update_bit) != 0;
        }
        static bool has_row_cell(const TransItem& item) {
            return (item.flags() & row_cell_bit) != 0;
        }
        static bool is_phantom(internal_elem *e, const TransItem& item) {
            return (!e->valid() && !has_insert(item));
        }


        bool _remove(const key_type& key) {
            auto row_iter = table_.find(key);
            if (row_iter != table_.end()) {
                auto e = *row_iter;
                assert(!(e->valid()));
                e->deleted = true;
                Transaction::rcu_delete(e);
                return true;
            }
            return false;
        }


        static bool is_ttnv(TransItem& item) {
            return (item.key<uintptr_t>() & ttnv_bit);
        }

        static void copy_row(internal_elem *e, comm_type &comm) {
            comm.operate(e->row_container.row);
        }
        static void copy_row(internal_elem *e, const value_type *new_row) {
            if (new_row == nullptr)
                return;
            e->row_container.row = *new_row;
        }
    };




} // namespace bench
#endif
