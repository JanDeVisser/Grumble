package grumble

import (
	"fmt"
	"math"
	"reflect"
	"testing"
)

const PRODUCT_KIND = "github.com.jandevisser.grumble.product"
const FRUIT_KIND = "github.com.jandevisser.grumble.fruit"
const SALE_KIND = "github.com.jandevisser.grumble.sale"
const SAMPLE_ID = 42

const VEGETABLE_CAT = "Vegetable"
const FRUIT_CAT = "Fruit"

const SQUASH = "Squash"
const SQUASH_PRICE = 1.25
const SQUASH_PRICE_NEW = SQUASH_PRICE * 1.05
const APPLE = "Apple"
const APPLE_PRICE = 0.69
const APPLE_COLOR = "Red"

const SQUASH_SALE_QUANTITY = 3
const APPLE_SALE_QUANTITY = 12
const APPLE_SALE_QUANTITY_2 = 13

func CompareFloats(a, b float64) bool {
	return math.Abs(a-b) < 0.00001
}

type Product struct {
	Key
	Name          string  `grumble:"persist=true"`
	Category      string  `grumble:"persist=true"`
	Price         float64 `grumble:"persist"`
	TotalQuantity int
	TotalAmount   float64
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
	product.Name = name
	product.Category = category
	product.Price = price
	err = Put(product)
	return
}

func (p *Product) AveragePrice() float64 {
	return math.Pi
}

type Fruit struct {
	Product
	Color string `grumble:"persist"`
}

func (f *Fruit) String() string {
	return fmt.Sprintf("%s %s (%5.2f)", f.Color, f.Name, f.Price)
}

func CreateFruit(name string, color string, price float64) (fruit *Fruit, err error) {
	fruit = &Fruit{
		Product: Product{
			Name:     name,
			Category: FRUIT_CAT,
			Price:    price,
		},
		Color: color,
	}
	err = Put(fruit)
	return
}

type Sale struct {
	Key
	Quantity int      `grumble:"persist"`
	Product  *Product `grumble:"persist"`
}

func CreateSale(product *Product, quantity int) (sale *Sale, err error) {
	sale = &Sale{
		Quantity: quantity,
		Product:  product,
	}
	err = Put(sale)
	return
}

var SquashID int
var AppleID int

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

func TestGetKindForType(t *testing.T) {
	k := GetKindForType(reflect.TypeOf(Product{}))
	if k == nil {
		t.Fatal("Could not get kind for Product")
	}
	if k.Name() != PRODUCT_KIND {
		t.Errorf("Name '%s' != '%s'", k.Name(), PRODUCT_KIND)
	}
}

func TestGetKindForType_Subclass(t *testing.T) {
	k := GetKindForType(reflect.TypeOf(Fruit{}))
	if k == nil {
		t.Fatal("Could not get kind for Fruit")
	}
	if k.Name() != FRUIT_KIND {
		t.Errorf("Name '%s' != '%s'", k.Name(), FRUIT_KIND)
	}
}

func TestGetKindForType_Reference(t *testing.T) {
	k := GetKindForType(reflect.TypeOf(Sale{}))
	if k == nil {
		t.Fatal("Could not get kind for Sale")
	}
}

func TestGetKindForKind(t *testing.T) {
	k := GetKindForKind(PRODUCT_KIND)
	if k == nil {
		t.Fatal("Can't get Kind")
	}
	if k.Name() != PRODUCT_KIND {
		t.Errorf("Name '%s' != '%s'", k.Name(), PRODUCT_KIND)
	}
}

func TestCreateKey(t *testing.T) {
	key, err := CreateKey("", GetKindForKind(PRODUCT_KIND), SAMPLE_ID)
	if err != nil {
		t.Fatal(err)
	}
	id := key.Id()
	if id != SAMPLE_ID {
		t.Errorf("Name '%d' != '%d'", id, SAMPLE_ID)
	}
}

func TestEntity_SetKind(t *testing.T) {
	product := Product{}
	SetKind(&product)
	if product.Kind().Kind != PRODUCT_KIND {
		t.Fatalf("Kind name '%s' != '%s'", product.Kind().Kind, PRODUCT_KIND)
	}

	productPtr := &Product{}
	SetKind(productPtr)
	if productPtr.Kind().Kind != PRODUCT_KIND {
		t.Fatalf("Kind name '%s' != '%s' [Pointer]", productPtr.Kind().Kind, PRODUCT_KIND)
	}

	k := GetKindForKind(PRODUCT_KIND)
	key, err := CreateKey("", GetKindForKind(PRODUCT_KIND), SAMPLE_ID)
	if err != nil {
		t.Fatal(err)
	}
	persistable, err := k.New(key)
	if err != nil {
		t.Fatal(err)
	}
	if persistable.Kind().Kind != PRODUCT_KIND {
		t.Fatalf("Kind name '%s' != '%s' [k.New]", persistable.Kind().Kind, PRODUCT_KIND)
	}
}

func TestNewEntityNoParent(t *testing.T) {
	k := GetKindForKind(PRODUCT_KIND)
	if k == nil {
		t.Fatal("Can't get Kind")
	}
	e, err := k.Make("", SAMPLE_ID)
	if err != nil {
		t.Fatalf("Can't create entity: %s", err)
	}
	product, ok := e.(*Product)
	if !ok {
		t.Fatal("Could not cast created Entity to Product")
	}
	if id := product.Id(); id != SAMPLE_ID {
		t.Fatalf("Product Id() '%d' != '%d'", id, SAMPLE_ID)
	}
	if !CompareFloats(product.AveragePrice(), math.Pi) {
		t.Fatal("product.AveragePrice failed")
	}
}

func TestPut_insert(t *testing.T) {
	product := &Product{}
	product.Name = SQUASH
	product.Category = VEGETABLE_CAT
	product.Price = SQUASH_PRICE
	if err := Put(product); err != nil {
		t.Fatalf("Could not persist product entity: %s", err)
	}
	SquashID = product.Id()
}

func TestGet_1(t *testing.T) {
	squash := &Product{}
	_, err := Get(squash, SquashID)
	if err != nil {
		t.Fatalf("Could not Get(%d): %s", SquashID, err)
	}
	if squash.Kind().Kind != PRODUCT_KIND {
		t.Errorf("Entity does not have proper Kind after Get: '%s' != '%s'", squash.Kind().Kind, PRODUCT_KIND)
	}
	if !CompareFloats(squash.Price, SQUASH_PRICE) {
		t.Fatalf("Entity's fields not restored properly: %f != %f", squash.Price, SQUASH_PRICE)
	}
}

func TestPut_update(t *testing.T) {
	squash := &Product{}
	_, err := Get(squash, SquashID)
	if err != nil {
		t.Fatalf("Could not Get(%d): %s", SquashID, err)
	}
	squash.Price = SQUASH_PRICE_NEW
	if err := Put(squash); err != nil {
		t.Fatalf("Could not persist squash entity: %s", err)
	}
}

func TestGet_2(t *testing.T) {
	squash := &Product{}
	_, err := Get(squash, SquashID)
	if err != nil {
		t.Fatalf("Could not Get(%d): %s", SquashID, err)
	}
	if !CompareFloats(squash.Price, SQUASH_PRICE_NEW) {
		t.Fatalf("Entity's fields not restored properly: %f != %f", squash.Price, SQUASH_PRICE_NEW)
	}
}

func TestGet_ByKey(t *testing.T) {
	key, err := CreateKey("", GetKindForKind(PRODUCT_KIND), SquashID)
	if err != nil {
		t.Fatal(err)
	}
	var squash *Product
	e, err := Get(key, SquashID)
	if err != nil {
		t.Fatalf("Could not Get(%d) by Key: %s", SquashID, err)
	}
	squash, ok := e.(*Product)
	if !ok {
		t.Fatalf("Could not cast Persistable returned by Get() by key")
	}
	if !CompareFloats(squash.Price, SQUASH_PRICE_NEW) {
		t.Fatalf("Entity's fields not restored properly: %f != %f", squash.Price, SQUASH_PRICE_NEW)
	}
}

func TestMakeQuery(t *testing.T) {
	q := MakeQuery(&Product{})
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
	if !CompareFloats(squash.Price, SQUASH_PRICE_NEW) {
		t.Fatalf("Entity's fields not restored properly: %f != %f", squash.Price, SQUASH_PRICE_NEW)
	}
}

func TestPut_insertDerived(t *testing.T) {
	fruit, err := CreateFruit(APPLE, APPLE_COLOR, APPLE_PRICE)
	if err != nil {
		t.Fatal(err)
	}
	AppleID = fruit.Id()
	if err := Put(fruit); err != nil {
		t.Fatalf("Could not persist fruit entity: %s", err)
	}
}

func TestMakeQuery_WithDerived(t *testing.T) {
	q := MakeQuery(&Product{})
	q.WithDerived = true
	q.AddFilter("Category", FRUIT_CAT)
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
	if !CompareFloats(fruit.Price, APPLE_PRICE) || fruit.Category != FRUIT_CAT {
		t.Fatalf("Entity's fields not restored properly: %f != %f, %s != %s",
			fruit.Price, APPLE_PRICE, fruit.Category, FRUIT_CAT)
	}
}

func TestPut_insertReference(t *testing.T) {
	fruit := &Fruit{}
	fruit.Initialize("", AppleID)
	SetKind(fruit)
	sale, err := CreateSale(&fruit.Product, APPLE_SALE_QUANTITY)
	if err != nil {
		t.Fatal(err)
	}
	if err := Put(sale); err != nil {
		t.Fatalf("Could not persist Sale entity: %s", err)
	}
}

func TestMakeQuery_WithReference(t *testing.T) {
	q := MakeQuery(&Sale{})
	q.WithDerived = true
	q.AddFilter("Quantity", APPLE_SALE_QUANTITY)
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
	if sale.Quantity != APPLE_SALE_QUANTITY {
		t.Fatalf("Entity's fields not restored properly: %d != %d",
			sale.Quantity, APPLE_SALE_QUANTITY)
	}
	product, err := Get(sale.Product, sale.Product.Id())
	if err != nil {
		t.Fatal(err)
	}
	fruit, ok := product.(*Fruit)
	if !ok {
		t.Fatal("Product reference is not a Fruit")
	}
	if fruit.Category != FRUIT_CAT {
		t.Fatalf("Referred entity's fields not restored properly: %s != %s", fruit.Category, FRUIT_CAT)
	}
}

func TestMakeQuery_WithJoin(t *testing.T) {
	q := MakeQuery(&Sale{})
	q.WithDerived = true
	q.AddFilter("Quantity", APPLE_SALE_QUANTITY)
	join := Join{QueryTable: QueryTable{Kind: GetKindForKind(PRODUCT_KIND), WithDerived: true}, FieldName: "Product"}
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
		t.Fatalf("Could not cast Persistable to proper Kind '%s'", SALE_KIND)
	}
	if sale.Quantity != APPLE_SALE_QUANTITY {
		t.Fatalf("Entity's fields not restored properly: %d != %d",
			sale.Quantity, APPLE_SALE_QUANTITY)
	}
	product, ok := row[1].(*Fruit)
	if !ok {
		t.Fatalf("Could not cast Persistable to proper Kind '%s'", FRUIT_KIND)
	}
	if product.Id() != sale.Product.Id() {
		t.Fatalf("Joined in entity and reference field have different IDs: %d != %d",
			product.Id(), sale.Product.Id())
	}
	if product.Category != FRUIT_CAT {
		t.Fatalf("Referred entity's fields not restored properly: %s != %s", product.Category, FRUIT_CAT)
	}
}

func TestMakeQuery_OuterJoin(t *testing.T) {
	q := MakeQuery(&Product{})
	q.WithDerived = true
	q.AddFilter("Category", FRUIT_CAT)
	join := Join{QueryTable: QueryTable{Kind: GetKind(&Sale{})}, FieldName: "Product", JoinType: Outer}
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
		t.Fatalf("Could not cast Persistable to proper Kind '%s'", FRUIT_KIND)
	}
	if product.Category != FRUIT_CAT {
		t.Fatalf("Referred entity's fields not restored properly: %s != %s", product.Category, FRUIT_CAT)
	}
	sale, ok := row[1].(*Sale)
	if !ok {
		t.Fatalf("Could not cast Persistable to proper Kind '%s'", SALE_KIND)
	}
	if product.Id() != sale.Product.Id() {
		t.Fatalf("Joined in entity and reference field have different IDs: %d != %d",
			product.Id(), sale.Product.Id())
	}
	if sale.Quantity != APPLE_SALE_QUANTITY {
		t.Fatalf("Entity's fields not restored properly: %d != %d",
			sale.Quantity, APPLE_SALE_QUANTITY)
	}
}

func TestMakeQuery_OuterJoin_NoMatch(t *testing.T) {
	q := MakeQuery(&Product{})
	q.WithDerived = true
	q.AddFilter("Category", VEGETABLE_CAT)
	join := Join{QueryTable: QueryTable{Kind: GetKind(&Sale{})}, FieldName: "Product", JoinType: Outer}
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
		t.Fatalf("Could not cast Persistable to proper Kind '%s'", PRODUCT_KIND)
	}
	if product.Category != VEGETABLE_CAT {
		t.Fatalf("Referred entity's fields not restored properly: %s != %s", product.Category, VEGETABLE_CAT)
	}
	sale := row[1]
	if sale.Kind() != nil {
		t.Fatalf("Expected a zero key, got '%s'", sale.Kind().Kind)
	}
}

func TestMakeQuery_OuterJoin_Aggregate(t *testing.T) {
	fruit := &Fruit{}
	fruit.Initialize("", AppleID)
	SetKind(fruit)
	_, err := CreateSale(&fruit.Product, APPLE_SALE_QUANTITY_2)
	if err != nil {
		t.Fatal(err)
	}
	squash := &Product{}
	squash.Initialize("", SquashID)
	SetKind(squash)
	_, err = CreateSale(squash, SQUASH_SALE_QUANTITY)
	if err != nil {
		t.Fatal(err)
	}

	q := MakeQuery(&Product{})
	q.WithDerived = true
	q.AddFilter("Category", FRUIT_CAT)
	join := Join{QueryTable: QueryTable{Kind: GetKind(&Sale{})}, FieldName: "Product", JoinType: Outer}
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
		t.Fatalf("Could not cast Persistable to proper Kind '%s'", PRODUCT_KIND)
	}
	if product.Category != FRUIT_CAT {
		t.Fatalf("Referred entity's fields not restored properly: %s != %s", product.Category, FRUIT_CAT)
	}
	t.Log(product.TotalQuantity)
}
