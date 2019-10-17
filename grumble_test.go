package grumble

import (
	"fmt"
	"math"
	"reflect"
	"testing"
)

const ProductKind = "github.com.jandevisser.grumble.product"
const FruitKind = "github.com.jandevisser.grumble.fruit"
const SaleKind = "github.com.jandevisser.grumble.sale"
const SelfRefKind = "github.com.jandevisser.grumble.selfref"
const SampleId = 42

const VegetableCat = "Vegetable"
const FruitCat = "Fruit"

const Squash = "Squash"
const SquashPrice = 1.25
const SquashPriceNew = SquashPrice * 1.05
const Apple = "Apple"
const ApplePrice = 0.69
const AppleColor = "Red"

const SquashSaleQuantity = 3
const AppleSaleQuantity = 12
const AppleSaleQuantity2 = 13
const NilSaleQuantity = 0

func CompareFloats(a, b float64) bool {
	return math.Abs(a-b) < 0.00001
}

type Department struct {
	Key
	Name        string
	Description string
}

var mgr *EntityManager

func TestMakeEntityManager(t *testing.T) {
	var err error
	mgr, err = MakeEntityManager()
	if err != nil {
		t.Fatal(err)
	}
}

func CreateDepartment(parent *Department, name string, description string) (department *Department, err error) {
	var p *Key
	if parent != nil {
		p = parent.AsKey()
	}
	e, err := GetKind(&Department{}).New(p)
	if err != nil {
		return
	}
	department, _ = e.(*Department)
	department.Name = name
	department.Description = description
	err = mgr.Put(department)
	return
}

type Product struct {
	Key
	Name          string
	Category      string
	Price         float64
	TotalQuantity int     `grumble:"transient"`
	TotalAmount   float64 `grumble:"transient"`
	Grade         string  `grumble:"transient"`
}

func (p *Product) String() string {
	return fmt.Sprintf("%s (%5.2f)", p.Name, p.Price)
}

func CreateProduct(name string, category string, price float64) (product *Product, err error) {
	product = &Product{
		Name:     name,
		Category: category,
		Price:    price,
	}
	err = mgr.Put(product)
	return
}

func (p *Product) AveragePrice() float64 {
	return math.Pi
}

type Fruit struct {
	Product
	Color string
}

func (f *Fruit) String() string {
	return fmt.Sprintf("%s %s (%5.2f)", f.Color, f.Name, f.Price)
}

func CreateFruit(name string, color string, price float64) (fruit *Fruit, err error) {
	fruit = &Fruit{
		Product: Product{
			Name:     name,
			Category: FruitCat,
			Price:    price,
		},
		Color: color,
	}
	err = mgr.Put(fruit)
	return
}

type Sale struct {
	Key
	Quantity int
	Product  *Product
}

func CreateSale(product *Product, quantity int) (sale *Sale, err error) {
	sale = &Sale{
		Quantity: quantity,
		Product:  product,
	}
	err = mgr.Put(sale)
	return
}

var SquashID int
var AppleID int
var SaleID int

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

func TestGetKindForType(t *testing.T) {
	k := GetKind(reflect.TypeOf(Product{}))
	if k == nil {
		t.Fatal("Could not get kind for Product")
	}
	if k.Name() != ProductKind {
		t.Errorf("Name '%s' != '%s'", k.Name(), ProductKind)
	}
}

func TestGetKindForType_Subclass(t *testing.T) {
	k := GetKind(reflect.TypeOf(Fruit{}))
	if k == nil {
		t.Fatal("Could not get kind for Fruit")
	}
	if k.Name() != FruitKind {
		t.Errorf("Name '%s' != '%s'", k.Name(), FruitKind)
	}
}

func TestGetKindForType_Reference(t *testing.T) {
	k := GetKind(reflect.TypeOf(Sale{}))
	if k == nil {
		t.Fatal("Could not get kind for Sale")
	}
}

type SelfRef struct {
	Key
	SelfRef *SelfRef
}

func TestGetKind_SelfRef(t *testing.T) {
	k := GetKind(SelfRef{})
	if k == nil {
		t.Fatal("Could not get kind for SelfRef")
	}
	col, ok := k.Column("SelfRef")
	if !ok {
		t.Fatal("Column SelfRef not found")
	}
	converter := col.Converter.(*ReferenceConverter)
	if converter.References.Kind != SelfRefKind {
		t.Fatal("SelfRef.SelfRef doesn't reference SelfRef")
	}
}

func TestGetKindForKind(t *testing.T) {
	k := GetKind(ProductKind)
	if k == nil {
		t.Fatal("Can't get Kind")
	}
	if k.Name() != ProductKind {
		t.Errorf("Name '%s' != '%s'", k.Name(), ProductKind)
	}
}

func TestCreateKey(t *testing.T) {
	key, err := CreateKey(nil, GetKind(ProductKind), SampleId)
	if err != nil {
		t.Fatal(err)
	}
	id := key.Id()
	if id != SampleId {
		t.Errorf("Name '%d' != '%d'", id, SampleId)
	}
}

func TestEntity_SetKind(t *testing.T) {
	product := Product{}
	SetKind(&product)
	if product.Kind().Kind != ProductKind {
		t.Fatalf("Kind name '%s' != '%s'", product.Kind().Kind, ProductKind)
	}

	productPtr := &Product{}
	SetKind(productPtr)
	if productPtr.Kind().Kind != ProductKind {
		t.Fatalf("Kind name '%s' != '%s' [Pointer]", productPtr.Kind().Kind, ProductKind)
	}

	k := GetKind(ProductKind)
	key, err := CreateKey(nil, GetKind(ProductKind), SampleId)
	if err != nil {
		t.Fatal(err)
	}
	persistable, err := k.New(key)
	if err != nil {
		t.Fatal(err)
	}
	if persistable.Kind().Kind != ProductKind {
		t.Fatalf("Kind name '%s' != '%s' [k.New]", persistable.Kind().Kind, ProductKind)
	}
}

func TestNewEntityNoParent(t *testing.T) {
	k := GetKind(ProductKind)
	if k == nil {
		t.Fatal("Can't get Kind")
	}
	e, err := mgr.Make(k, nil, SampleId)
	if err != nil {
		t.Fatalf("Can't create entity: %s", err)
	}
	product, ok := e.(*Product)
	if !ok {
		t.Fatal("Could not cast created Entity to Product")
	}
	if id := product.Id(); id != SampleId {
		t.Fatalf("Product Id() '%d' != '%d'", id, SampleId)
	}
	if !CompareFloats(product.AveragePrice(), math.Pi) {
		t.Fatal("product.AveragePrice failed")
	}
}

func TestPut_insert(t *testing.T) {
	product := &Product{}
	product.Name = Squash
	product.Category = VegetableCat
	product.Price = SquashPrice
	if err := mgr.Put(product); err != nil {
		t.Fatalf("Could not persist product entity: %s", err)
	}
	SquashID = product.Id()
}

func TestGet_1(t *testing.T) {
	e, err := mgr.Get(&Product{}, SquashID)
	if err != nil {
		t.Fatalf("Could not Get(Product:%d): %s", SquashID, err)
	}
	squash, ok := e.(*Product)
	if !ok {
		t.Fatalf("Could not cast Persistable %s:%d to Product", e.Kind().Kind, e.Id())
	}
	if squash.Kind().Kind != ProductKind {
		t.Errorf("Entity does not have proper Kind after Get: '%s' != '%s'", squash.Kind().Kind, ProductKind)
	}
	if !CompareFloats(squash.Price, SquashPrice) {
		t.Fatalf("Entity's fields not restored properly: %f != %f", squash.Price, SquashPrice)
	}
}

func TestInflate(t *testing.T) {
	squash := &Product{}
	squash.Initialize(nil, SquashID)
	err := mgr.Inflate(squash)
	if err != nil {
		t.Fatalf("Could not Inflate: %s", err)
	}
	if squash.Kind().Kind != ProductKind {
		t.Errorf("Entity does not have proper Kind after Get: '%s' != '%s'", squash.Kind().Kind, ProductKind)
	}
	if !CompareFloats(squash.Price, SquashPrice) {
		t.Fatalf("Entity's fields not restored properly: %f != %f", squash.Price, SquashPrice)
	}
}

func TestPut_update(t *testing.T) {
	squash := &Product{}
	squash.Initialize(nil, SquashID)
	err := mgr.Inflate(squash)
	if err != nil {
		t.Fatalf("Could not Inflate: %s", err)
	}
	squash.Price = SquashPriceNew
	if err := mgr.Put(squash); err != nil {
		t.Fatalf("Could not persist squash entity: %s", err)
	}
}

func TestInflate_2(t *testing.T) {
	squash := &Product{}
	squash.Initialize(nil, SquashID)
	err := mgr.Inflate(squash)
	if err != nil {
		t.Fatalf("Could not Inflate: %s", err)
	}
	if !CompareFloats(squash.Price, SquashPriceNew) {
		t.Fatalf("Entity's fields not restored properly: %f != %f", squash.Price, SquashPriceNew)
	}
}

func TestGet_ByKey(t *testing.T) {
	key, err := CreateKey(nil, GetKind(ProductKind), SquashID)
	if err != nil {
		t.Fatal(err)
	}
	var squash *Product
	e, err := mgr.Get(key, SquashID)
	if err != nil {
		t.Fatalf("Could not Get(%d) by Key: %s", SquashID, err)
	}
	squash, ok := e.(*Product)
	if !ok {
		t.Fatalf("Could not cast Persistable returned by Get() by key")
	}
	if !CompareFloats(squash.Price, SquashPriceNew) {
		t.Fatalf("Entity's fields not restored properly: %f != %f", squash.Price, SquashPriceNew)
	}
}

func TestMakeQuery(t *testing.T) {
	q := mgr.MakeQuery(&Product{})
	q.AddFilter("Category", "Vegetable")
	q.AddFilter("Bogus", 42)
	results, err := q.Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatalf("No results")
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 results, got %d", len(results))
	}
	row := results[0]
	squash, ok := row[0].(*Product)
	if !ok {
		t.Fatalf("Could not cast Persistable returned by Get() by key")
	}
	if !CompareFloats(squash.Price, SquashPriceNew) {
		t.Fatalf("Entity's fields not restored properly: %f != %f", squash.Price, SquashPriceNew)
	}
}

func TestPut_insertDerived(t *testing.T) {
	fruit, err := CreateFruit(Apple, AppleColor, ApplePrice)
	if err != nil {
		t.Fatal(err)
	}
	AppleID = fruit.Id()
	if err := mgr.Put(fruit); err != nil {
		t.Fatalf("Could not persist fruit entity: %s", err)
	}
}

func TestMakeQuery_WithDerived(t *testing.T) {
	q := mgr.MakeQuery(&Product{})
	q.WithDerived = true
	q.AddFilter("Category", FruitCat)
	results, err := q.Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatalf("No results")
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 results, got %d", len(results))
	}
	row := results[0]
	fruit, ok := row[0].(*Fruit)
	if !ok {
		t.Fatalf("Expected a Fruit, got a '%s'", row[0].Kind().Kind)
	}
	if !CompareFloats(fruit.Price, ApplePrice) || fruit.Category != FruitCat {
		t.Fatalf("Entity's fields not restored properly: %f != %f, %s != %s",
			fruit.Price, ApplePrice, fruit.Category, FruitCat)
	}
}

func TestPut_insertReference(t *testing.T) {
	fruit := &Fruit{}
	fruit.Initialize(nil, AppleID)
	SetKind(fruit)
	sale, err := CreateSale(&fruit.Product, AppleSaleQuantity)
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.Put(sale); err != nil {
		t.Fatalf("Could not persist Sale entity: %s", err)
	}
	SaleID = sale.Id()
}

func TestGet_WithReference(t *testing.T) {
	e, err := mgr.Get(&Sale{}, SaleID)
	if err != nil {
		t.Fatal(err)
	}
	sale := e.(*Sale)
	if sale.Product.Name != Apple {
		t.Fatalf("Product is not %q but %q", Apple, sale.Product.Name)
	}
}

func TestPut_insertNilReference(t *testing.T) {
	sale, err := CreateSale(nil, NilSaleQuantity)
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.Put(sale); err != nil {
		t.Fatalf("Could not persist Sale entity: %s", err)
	}
}

func TestMakeQuery_WithReference(t *testing.T) {
	q := mgr.MakeQuery(&Sale{})
	q.WithDerived = true
	q.AddFilter("Quantity", AppleSaleQuantity)
	results, err := q.Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatalf("No results")
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 results, got %d", len(results))
	}
	row := results[0]
	sale, ok := row[0].(*Sale)
	if !ok {
		t.Fatalf("Could not cast Persistable to proper Kind")
	}
	if sale.Quantity != AppleSaleQuantity {
		t.Fatalf("Entity's fields not restored properly: %d != %d",
			sale.Quantity, AppleSaleQuantity)
	}
	product, err := mgr.Get(sale.Product, sale.Product.Id())
	if err != nil {
		t.Fatal(err)
	}
	fruit, ok := product.(*Fruit)
	if !ok {
		t.Fatal("Product reference is not a Fruit")
	}
	if fruit.Category != FruitCat {
		t.Fatalf("Referred entity's fields not restored properly: %s != %s", fruit.Category, FruitCat)
	}
}

func TestMakeQuery_WithJoin(t *testing.T) {
	q := mgr.MakeQuery(&Sale{})
	q.WithDerived = true
	q.AddFilter("Quantity", AppleSaleQuantity)
	join := Join{QueryTable: QueryTable{Kind: GetKind(ProductKind), WithDerived: true}, FieldName: "Product"}
	q.AddJoin(join)
	results, err := q.Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatalf("No results")
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 results, got %d", len(results))
	}
	row := results[0]
	sale, ok := row[0].(*Sale)
	if !ok {
		t.Fatalf("Could not cast Persistable to proper Kind '%s'", SaleKind)
	}
	if sale.Quantity != AppleSaleQuantity {
		t.Fatalf("Entity's fields not restored properly: %d != %d",
			sale.Quantity, AppleSaleQuantity)
	}
	product, ok := row[1].(*Fruit)
	if !ok {
		t.Fatalf("Could not cast Persistable to proper Kind '%s'", FruitKind)
	}
	if product.Id() != sale.Product.Id() {
		t.Fatalf("Joined in entity and reference field have different IDs: %d != %d",
			product.Id(), sale.Product.Id())
	}
	if product.Category != FruitCat {
		t.Fatalf("Referred entity's fields not restored properly: %s != %s", product.Category, FruitCat)
	}
}

func TestMakeQuery_OuterJoin(t *testing.T) {
	q := mgr.MakeQuery(&Product{})
	q.WithDerived = true
	q.AddFilter("Category", FruitCat)
	join := Join{QueryTable: QueryTable{Kind: GetKind(&Sale{})}, FieldName: "Product", Direction: ReferredBy}
	q.AddJoin(join)
	results, err := q.Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatalf("No results")
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 results, got %d", len(results))
	}
	row := results[0]
	product, ok := row[0].(*Fruit)
	if !ok {
		t.Fatalf("Could not cast Persistable to proper Kind '%s'", FruitKind)
	}
	if product.Category != FruitCat {
		t.Fatalf("Referred entity's fields not restored properly: %s != %s", product.Category, FruitCat)
	}
	sale, ok := row[1].(*Sale)
	if !ok {
		t.Fatalf("Could not cast Persistable to proper Kind '%s'", SaleKind)
	}
	if product.Id() != sale.Product.Id() {
		t.Fatalf("Joined in entity and reference field have different IDs: %d != %d",
			product.Id(), sale.Product.Id())
	}
	if sale.Quantity != AppleSaleQuantity {
		t.Fatalf("Entity's fields not restored properly: %d != %d",
			sale.Quantity, AppleSaleQuantity)
	}
}

func TestMakeQuery_OuterJoin_NoMatch(t *testing.T) {
	q := mgr.MakeQuery(&Product{})
	q.WithDerived = true
	q.AddFilter("Category", VegetableCat)
	join := Join{QueryTable: QueryTable{Kind: GetKind(&Sale{})}, FieldName: "Product", Direction: ReferredBy, JoinType: Outer}
	q.AddJoin(join)
	results, err := q.Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatalf("No results")
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 results, got %d", len(results))
	}
	row := results[0]
	product, ok := row[0].(*Product)
	if !ok {
		t.Fatalf("Could not cast Persistable to proper Kind '%s'", ProductKind)
	}
	if product.Category != VegetableCat {
		t.Fatalf("Referred entity's fields not restored properly: %s != %s", product.Category, VegetableCat)
	}
	sale := row[1]
	if sale.Kind() != nil {
		t.Fatalf("Expected a zero key, got '%s'", sale.Kind().Kind)
	}
}

func TestMakeQuery_OuterJoin_Aggregate(t *testing.T) {
	fruit := &Fruit{}
	Initialize(fruit, nil, AppleID)
	_, err := CreateSale(&fruit.Product, AppleSaleQuantity2)
	if err != nil {
		t.Fatal(err)
	}
	squash := &Product{}
	squash.Initialize(nil, SquashID)
	SetKind(squash)
	_, err = CreateSale(squash, SquashSaleQuantity)
	if err != nil {
		t.Fatal(err)
	}

	q := mgr.MakeQuery(&Product{})
	q.WithDerived = true
	q.AddFilter("Category", FruitCat)
	join := Join{QueryTable: QueryTable{Kind: GetKind(&Sale{})}, FieldName: "Product", Direction: ReferredBy}
	join.AddAggregate(Aggregate{
		Function: "SUM",
		Column:   "Quantity",
		Name:     "TotalQuantity",
		Default:  "0",
	})
	q.AddJoin(join)
	q.GroupBy = true

	results, err := q.Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatalf("No results")
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 results, got %d", len(results))
	}
	row := results[0]
	var product *Product
	switch e := row[0].(type) {
	case *Product:
		product = e
	case *Fruit:
		product = &e.Product
	default:
		t.Fatalf("Could not cast Persistable to proper Kind '%s'", ProductKind)
	}
	if product.Category != FruitCat {
		t.Fatalf("Queried entity's fields not restored properly: %s != %s", product.Category, FruitCat)
	}
	if product.TotalQuantity != (AppleSaleQuantity + AppleSaleQuantity2) {
		t.Fatalf("Queried entity's aggregated field not restored properly: %d != %d",
			product.TotalQuantity, AppleSaleQuantity+AppleSaleQuantity2)
	}
}

var GroceriesID int

func TestPut_Hierarchy(t *testing.T) {
	groceries, err := CreateDepartment(nil, "Groceries", "Grocery Store")
	if err != nil {
		t.Fatal(err)
	}
	GroceriesID = groceries.Id()
	_, err = CreateDepartment(groceries, "Fruits and Vegetables", "Fruits and Vegetables")
	if err != nil {
		t.Fatal(err)
	}
}

func TestQuery_Hierarchy(t *testing.T) {
	groceries := &Department{}
	Initialize(groceries, nil, GroceriesID)
	q := mgr.MakeQuery(groceries)
	q.WithDerived = true
	q.HasParent(groceries)
	results, err := q.Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatalf("No results")
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 results, got %d", len(results))
	}
	row := results[0]
	var department *Department
	switch e := row[0].(type) {
	case *Department:
		department = e
	default:
		t.Fatalf("Could not cast Persistable to proper Kind '%s'", groceries.Kind().Kind)
	}
	if department.Name != "Fruits and Vegetables" {
		t.Fatalf("Descendent entity's fields not restored properly: %s != %s",
			department.Name, "Fruits and Vegetables")
	}
}

func TestQuery_Computed(t *testing.T) {
	q := mgr.MakeQuery(&Product{})
	q.WithDerived = true
	q.AddComputedColumn(Computed{
		Formula: "(CASE WHEN \"Price\" > 1 THEN 'Expensive' ELSE 'Cheap' END)",
		Name:    "Grade",
	})
	results, err := q.Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatalf("No results")
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
	for _, row := range results {
		switch e := row[0].(type) {
		case *Fruit:
			t.Logf("Fruit '%s', Grade '%s'", e.Name, e.Grade)
		case *Product:
			t.Logf("Product '%s', Grade '%s'", e.Name, e.Grade)
		}
	}
}
