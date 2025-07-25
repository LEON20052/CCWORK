CREATE TABLE "Vehicles"(
    "vehicles_id" VARCHAR(255) NOT NULL,
    "vehicle_type" VARCHAR(100) NOT NULL,
    "vehicle_nickname" VARCHAR(100) NOT NULL,
    "fleet_number" INTEGER NOT NULL,
    "capacity" INTEGER NOT NULL,
    "registration_number" VARCHAR(50) NULL,
    "status" VARCHAR(255) CHECK
        ("status" IN('')) NULL DEFAULT 'Active',
        "created_at" TIMESTAMP(0) WITHOUT TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP,
        "updated_at" TIMESTAMP(0) WITHOUT TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE
    "Vehicles" ADD PRIMARY KEY("vehicles_id");
ALTER TABLE
    "Vehicles" ADD CONSTRAINT "vehicles_fleet_number_unique" UNIQUE("fleet_number");
CREATE TABLE "Staff"(
    "staff_id" INTEGER NOT NULL,
    "role" VARCHAR(255) CHECK
        ("role" IN('')) NOT NULL,
        "name" VARCHAR(255) NOT NULL,
        "phone_contact" VARCHAR(20) NOT NULL,
        "shifts" VARCHAR(255)
    CHECK
        ("shifts" IN('')) NOT NULL,
        "vehicles_id" VARCHAR(255) NOT NULL,
        "hire_date" DATE NULL,
        "status" VARCHAR(255)
    CHECK
        ("status" IN('')) NULL DEFAULT 'Active',
        "created_at" TIMESTAMP(0) WITHOUT TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP,
        "updated_at" TIMESTAMP(0) WITHOUT TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE
    "Staff" ADD PRIMARY KEY("staff_id");
CREATE INDEX "staff_role_index" ON
    "Staff"("role");
CREATE INDEX "staff_status_index" ON
    "Staff"("status");
CREATE TABLE "Routes"(
    "route_id" BIGINT NOT NULL,
    "route_name" VARCHAR(255) NOT NULL,
    "origin" VARCHAR(255) NOT NULL,
    "destination" VARCHAR(255) NOT NULL,
    "distance_km" DECIMAL(6, 2) NOT NULL,
    "standard_fare" DECIMAL(8, 2) NOT NULL,
    "estimated_time" TIME(0) WITHOUT TIME ZONE NOT NULL,
    "status" VARCHAR(255) CHECK
        ("status" IN('')) NULL DEFAULT 'Active',
        "created_at" TIMESTAMP(0) WITHOUT TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP,
        "updated_at" TIMESTAMP(0) WITHOUT TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX "routes_origin_destination_index" ON
    "Routes"("origin", "destination");
ALTER TABLE
    "Routes" ADD PRIMARY KEY("route_id");
CREATE INDEX "routes_status_index" ON
    "Routes"("status");
CREATE TABLE "Trip"(
    "trip_id" VARCHAR(255) NOT NULL,
    "trip_date" INTEGER NOT NULL,
    "departure_time" TIME(0) WITHOUT TIME ZONE NULL,
    "arrival_time" TIME(0) WITHOUT TIME ZONE NULL,
    "amount_collected" DECIMAL(10, 2) NOT NULL DEFAULT '0',
    "fuel_cost" DECIMAL(8, 2) NOT NULL DEFAULT '0',
    "route_id" BIGINT NOT NULL,
    "vehicles_id" VARCHAR(255) NOT NULL,
    "staff_id" INTEGER NOT NULL,
    "trip_status" VARCHAR(255) CHECK
        ("trip_status" IN('')) NULL DEFAULT 'Scheduled',
        "passenger_count" INTEGER NULL,
        "notes" TEXT NULL,
        "created_at" TIMESTAMP(0) WITHOUT TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP,
        "updated_at" TIMESTAMP(0) WITHOUT TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE
    "Trip" ADD PRIMARY KEY("trip_id");
CREATE INDEX "trip_trip_date_index" ON
    "Trip"("trip_date");
CREATE INDEX "trip_route_id_index" ON
    "Trip"("route_id");
CREATE INDEX "trip_vehicles_id_index" ON
    "Trip"("vehicles_id");
CREATE INDEX "trip_trip_status_index" ON
    "Trip"("trip_status");
CREATE TABLE "Travellers_table"(
    "travellers_id" BIGINT NOT NULL,
    "passenger_name" VARCHAR(255) NULL,
    "phone_number" VARCHAR(20) NULL,
    "booking_date" INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "seat_number" INTEGER NULL,
    "fare_paid" DECIMAL(8, 2) NOT NULL,
    "trip_id" VARCHAR(255) NOT NULL,
    "payment_status" VARCHAR(255) CHECK
        ("payment_status" IN('')) NULL DEFAULT 'Pending',
        "booking_reference" VARCHAR(50) NULL,
        "special_requirements" TEXT NULL,
        "created_at" TIMESTAMP(0) WITHOUT TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP,
        "updated_at" TIMESTAMP(0) WITHOUT TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE
    "Travellers_table" ADD CONSTRAINT "travellers_table_trip_id_seat_number_unique" UNIQUE("trip_id", "seat_number");
ALTER TABLE
    "Travellers_table" ADD PRIMARY KEY("travellers_id");
CREATE INDEX "travellers_table_booking_date_index" ON
    "Travellers_table"("booking_date");
CREATE INDEX "travellers_table_trip_id_index" ON
    "Travellers_table"("trip_id");
CREATE INDEX "travellers_table_payment_status_index" ON
    "Travellers_table"("payment_status");
CREATE TABLE "Payment"(
    "payment_id" BIGINT NOT NULL,
    "payment_method" VARCHAR(255) CHECK
        ("payment_method" IN('')) NOT NULL,
        "payment_date" INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP,
        "amount_paid" DECIMAL(10, 2) NOT NULL,
        "transaction_reference" VARCHAR(100) NULL,
        "travellers_id" BIGINT NOT NULL,
        "staff_id" INTEGER NOT NULL,
        "payment_status" VARCHAR(255)
    CHECK
        ("payment_status" IN('')) NULL DEFAULT 'Completed',
        "notes" TEXT NULL,
        "created_at" TIMESTAMP(0) WITHOUT TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP,
        "updated_at" TIMESTAMP(0) WITHOUT TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE
    "Payment" ADD PRIMARY KEY("payment_id");
CREATE INDEX "payment_payment_method_index" ON
    "Payment"("payment_method");
CREATE INDEX "payment_payment_date_index" ON
    "Payment"("payment_date");
CREATE INDEX "payment_travellers_id_index" ON
    "Payment"("travellers_id");
CREATE INDEX "payment_payment_status_index" ON
    "Payment"("payment_status");
CREATE TABLE "Maintenance"(
    "maintenance_id" BIGINT NOT NULL,
    "vehicles_id" VARCHAR(255) NOT NULL,
    "maintenance_type" VARCHAR(255) CHECK
        ("maintenance_type" IN('')) NOT NULL,
        "description" TEXT NOT NULL,
        "cost" DECIMAL(10, 2) NOT NULL,
        "maintenance_date" INTEGER NOT NULL,
        "mechanic_name" VARCHAR(255) NULL,
        "status" VARCHAR(255)
    CHECK
        ("status" IN('')) NULL DEFAULT 'Scheduled',
        "created_at" TIMESTAMP(0) WITHOUT TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP,
        "updated_at" TIMESTAMP(0) WITHOUT TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE
    "Maintenance" ADD PRIMARY KEY("maintenance_id");
CREATE INDEX "maintenance_vehicles_id_index" ON
    "Maintenance"("vehicles_id");
CREATE INDEX "maintenance_maintenance_date_index" ON
    "Maintenance"("maintenance_date");
CREATE TABLE "Expenses"(
    "expense_id" BIGINT NOT NULL,
    "expense_type" VARCHAR(255) CHECK
        ("expense_type" IN('')) NOT NULL,
        "description" TEXT NOT NULL,
        "amount" DECIMAL(10, 2) NOT NULL,
        "expense_date" INTEGER NOT NULL,
        "vehicles_id" VARCHAR(255) NULL,
        "staff_id" INTEGER NULL,
        "receipt_number" VARCHAR(100) NULL,
        "approved_by" INTEGER NULL,
        "status" VARCHAR(255)
    CHECK
        ("status" IN('')) NULL DEFAULT 'Pending',
        "created_at" TIMESTAMP(0) WITHOUT TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP,
        "updated_at" TIMESTAMP(0) WITHOUT TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE
    "Expenses" ADD PRIMARY KEY("expense_id");
CREATE INDEX "expenses_expense_type_index" ON
    "Expenses"("expense_type");
CREATE INDEX "expenses_expense_date_index" ON
    "Expenses"("expense_date");
CREATE INDEX "expenses_status_index" ON
    "Expenses"("status");
ALTER TABLE
    "Staff" ADD CONSTRAINT "staff_vehicles_id_foreign" FOREIGN KEY("vehicles_id") REFERENCES "Vehicles"("vehicles_id");
ALTER TABLE
    "Payment" ADD CONSTRAINT "payment_travellers_id_foreign" FOREIGN KEY("travellers_id") REFERENCES "Travellers_table"("travellers_id");
ALTER TABLE
    "Trip" ADD CONSTRAINT "trip_staff_id_foreign" FOREIGN KEY("staff_id") REFERENCES "Staff"("staff_id");
ALTER TABLE
    "Payment" ADD CONSTRAINT "payment_staff_id_foreign" FOREIGN KEY("staff_id") REFERENCES "Staff"("staff_id");
ALTER TABLE
    "Travellers_table" ADD CONSTRAINT "travellers_table_trip_id_foreign" FOREIGN KEY("trip_id") REFERENCES "Trip"("trip_id");
ALTER TABLE
    "Trip" ADD CONSTRAINT "trip_vehicles_id_foreign" FOREIGN KEY("vehicles_id") REFERENCES "Vehicles"("vehicles_id");
ALTER TABLE
    "Expenses" ADD CONSTRAINT "expenses_vehicles_id_foreign" FOREIGN KEY("vehicles_id") REFERENCES "Vehicles"("vehicles_id");
ALTER TABLE
    "Maintenance" ADD CONSTRAINT "maintenance_vehicles_id_foreign" FOREIGN KEY("vehicles_id") REFERENCES "Vehicles"("vehicles_id");
ALTER TABLE
    "Trip" ADD CONSTRAINT "trip_route_id_foreign" FOREIGN KEY("route_id") REFERENCES "Routes"("route_id");
ALTER TABLE
    "Expenses" ADD CONSTRAINT "expenses_staff_id_foreign" FOREIGN KEY("staff_id") REFERENCES "Staff"("staff_id");