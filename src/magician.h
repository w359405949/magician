#pragma once

#include <map>
#include <stack>
#include <google/protobuf/message.h>

#include "leveldb/db.h"
#include "uv.h"

namespace magician {

class Magician
{
public:
    Magician(uv_loop_t* loop=NULL);
    ~Magician();

    bool Open(std::string db_path);

    const google::protobuf::Message& GetModel(const google::protobuf::Descriptor* descriptor, const std::string& id, int depth=0);

    google::protobuf::Message* MutableModel(const google::protobuf::Descriptor* descriptor, const std::string& id, int depth=0, google::protobuf::* initial=NULL);

private:
    static void OnAutoSave(uv_idle_t* idle);

private:
    std::map<std::string, ::google::protobuf::Message*> cache_rw_;
    std::map<std::string, ::google::protobuf::Message*> cache_ro_;
    std::map<const google::protobuf::Descriptor*, ::google::protobuf::Message*> whipping_instance_;

    std::string id_string_;

    uv_idle_t auto_save_;
    uv_loop_t* loop_;

    leveldb::DB* db_;
};

} // magician
