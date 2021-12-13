#include <typeinfo>
#include "config.h"
#include "schema.hh"
#include "relmodel.hh"
#include <pqxx/pqxx>
#include "gitrev.h"

using namespace std;
using namespace pqxx;

void schema::generate_indexes() {

  cerr << "Generating indexes..." << endl;

  cerr << "Iterating over types..." << endl;

  for (auto &type: types) {
    assert(type);
    cerr << "Type: " << type->name << endl;

    cerr << "   Iterating over aggregates:" << endl;
    for(auto &r: aggregates) {
      if (type->consistent(r.restype))
	aggregates_returning_type.insert(pair<sqltype*, routine*>(type, &r));
      cerr << "      Aggregate: " << r.name << endl;
    }

    cerr << "   Iterating over routines:" << endl;
    for(auto &r: routines) {
      if (!type->consistent(r.restype))
	continue;
      routines_returning_type.insert(pair<sqltype*, routine*>(type, &r));
      cerr << "      Routine: " << r.name << endl;
      if(!r.argtypes.size())
	parameterless_routines_returning_type.insert(pair<sqltype*, routine*>(type, &r));
    }

    cerr << "   Iterating over tables:" << endl;
    for (auto &t: tables) {
      cerr << "      Table: " << t.name << endl;
      for (auto &c: t.columns()) {
	if (type->consistent(c.type)) {
	  tables_with_columns_of_type.insert(pair<sqltype*, table*>(type, &t));
      cerr << "         Column: "<< c.type->name << endl;
	  break;
	}
      }
    }

    cerr << "   Iterating over concrete types:" << endl;
    for (auto &concrete: types) {
      if (type->consistent(concrete)) {
          concrete_type.insert(pair<sqltype *, sqltype *>(type, concrete));
          cerr << "      Concrete type: " << concrete->name << ", type: "<< type->name << endl;
      }
    }

    cerr << "   Iterating over operators:" << endl;
    for (auto &o: operators) {
      if (type->consistent(o.result)) {
          operators_returning_type.insert(pair<sqltype *, op *>(type, &o));
          cerr << "      Operator: " << o.name << ", return type: "<< o.result->name << endl;
      }
    }
    cerr << "Done type: " << type->name << endl;
  }

  cerr << "Iterating over base tables..." << endl;
  for (auto &t: tables) {
    if (t.is_base_table)
      base_tables.push_back(&t);
  }
  
  cerr << "done." << endl;

  assert(booltype);
  assert(inttype);
  assert(internaltype);
  assert(arraytype);

}
