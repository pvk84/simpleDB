//
//  simpleDB.hpp
//  simpleDB
//
//  Created by vamsi on 2/15/16.
//  Copyright © 2016 VamsiKrishna. All rights reserved.
//

#ifndef simpleDB_hpp
#define simpleDB_hpp

#include <string>

#include <unordered_map> //or use a std::map which uses a BST for O(LOG N)
#include <stack>

namespace COMMAND{
    extern std::string UNKNOWN;
    extern std::string SET;
    extern std::string GET;
    extern std::string UNSET;
    extern std::string NUMEQUALTO;
    extern std::string END;
    
    extern std::string BEGIN;
    extern std::string COMMIT;
    extern std::string ROLLBACK;
};

class SimpleDB{
public:
    typedef std::tuple<std::string, std::string, std::string> CommandObj;
    typedef std::stack<CommandObj> CommandStack;
    
    SimpleDB();
    ~SimpleDB();
    void set(std::string key, std::string val);
    std::string get(std::string key);
    bool unset(std::string key);
    int numEqualTo(std::string val);
    
    void beginTransaction();
    bool commitTransaction();
    bool rollbackTransaction();
    
    static void run();
    
    
private:
    std::unordered_map<std::string, std::string> mDB;
    std::unordered_map<std::string, int> mCountMap;
    std::stack<CommandStack> mTransactionStack;
    bool inTransaction;
    
private:
    void execute(CommandObj commandObj);
};

#endif /* simpleDB_hpp */
