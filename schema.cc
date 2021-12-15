#include <typeinfo>
#include "config.h"
#include "schema.hh"
#include "relmodel.hh"
#include <pqxx/pqxx>
#include "gitrev.h"

using namespace std;
using namespace pqxx;
extern std::ostream * pg_mtd_info_stream;

void schema::generate_indexes() {

  cerr << "Generating indexes...";
  
  if(pg_mtd_info_stream == &cerr)
     (*pg_mtd_info_stream) << endl;

  (*pg_mtd_info_stream) << "Iterating over types..." << endl;

  for (auto &type: types) {
    assert(type);
    (*pg_mtd_info_stream) << "Type: " << type->name << endl;

    (*pg_mtd_info_stream) << "   Iterating over aggregates:" << endl;
    for(auto &r: aggregates) {
      if (type->consistent(r.restype))
	aggregates_returning_type.insert(pair<sqltype*, routine*>(type, &r));
      (*pg_mtd_info_stream) << "      Aggregate: " << r.name << endl;
    }

    (*pg_mtd_info_stream) << "   Iterating over routines:" << endl;
    for(auto &r: routines) {
      if (!type->consistent(r.restype))
	continue;
      routines_returning_type.insert(pair<sqltype*, routine*>(type, &r));
      (*pg_mtd_info_stream) << "      Routine: " << r.name << endl;
      if(!r.argtypes.size())
	parameterless_routines_returning_type.insert(pair<sqltype*, routine*>(type, &r));
    }

    (*pg_mtd_info_stream) << "   Iterating over tables:" << endl;
    for (auto &t: tables) {
      (*pg_mtd_info_stream) << "      Table: " << t.name << endl;
      for (auto &c: t.columns()) {
	if (type->consistent(c.type)) {
	  tables_with_columns_of_type.insert(pair<sqltype*, table*>(type, &t));
      (*pg_mtd_info_stream) << "         Column: "<< c.type->name << endl;
	  break;
	}
      }
    }

    (*pg_mtd_info_stream) << "   Iterating over concrete types:" << endl;
    for (auto &concrete: types) {
      if (type->consistent(concrete)) {
          concrete_type.insert(pair<sqltype *, sqltype *>(type, concrete));
          (*pg_mtd_info_stream) << "      Concrete type: " << concrete->name << ", type: "<< type->name << endl;
      }
    }

    (*pg_mtd_info_stream) << "   Iterating over operators:" << endl;
    for (auto &o: operators) {
      if (type->consistent(o.result)) {
          operators_returning_type.insert(pair<sqltype *, op *>(type, &o));
          (*pg_mtd_info_stream) << "      Operator: " << o.name << ", return type: "<< o.result->name << endl;
      }
    }
    (*pg_mtd_info_stream) << "Done type: " << type->name << endl;
  }

  (*pg_mtd_info_stream) << "Iterating over tables..." << endl;
  for (auto &t: tables) {
    if (t.is_base_table) {
        (*pg_mtd_info_stream) << "   Table: " << t.name << " added to base tables" << endl;
        base_tables.push_back(&t);
    }
  }
  
  cerr << "done." << endl;

  assert(booltype);
  assert(inttype);
  assert(internaltype);
  assert(arraytype);

}
