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

    google::protobuf::Message* MutableModel(const google::protobuf::Descriptor* descriptor, const std::string& id, int depth=0);

    const google::protobuf::Message& GetModel(const google::protobuf::Descriptor* descriptor, const std::string& id, int depth=0);

protected:
    virtual google::protobuf::Message* GenerateModel(const google::protobuf::Descriptor* descriptor, const std::string& key);
    virtual bool Save(std::map<std::string, ::google::protobuf::Message*> models);

private:
    static void OnAutoSave(uv_idle_t* handle);
    void SaveAll();

private:
    std::map<std::string, ::google::protobuf::Message*> cache_rw_;
    std::map<std::string, ::google::protobuf::Message*> cache_ro_;
    std::map<std::string, ::google::protobuf::Message*> cache_atomic_rw_;
    std::map<std::string, ::google::protobuf::Message*> cache_atomic_ro_;
    std::map<std::string, ::google::protobuf::Message*> cache_orig_;

    std::string id_string_;

    uv_idle_t auto_save_;
    uv_loop_t* loop_;
};


class LevelDBImpl : public Magician
{
public:
    static LevelDBImpl* Open(uv_loop_t* loop, std::string path);
    static void CloseAll();

private:
    static std::map<std::string, LevelDBImpl*> dbs_;

protected:
    virtual google::protobuf::Message* GenerateModel(const google::protobuf::Descriptor* descriptor, const std::string& key);
    virtual bool Save(std::map<std::string, ::google::protobuf::Messaeg*> models);

private:
    leveldb::DB* db_;
};

} // magician
