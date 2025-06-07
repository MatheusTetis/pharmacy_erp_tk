from datetime import datetime
from delta import *
import os
import pandas as pd
import pyspark
import pyspark.sql.functions as F
from request_medicine_price_data import download_medicine_pricing_table
from schemas import pharmacy_sales_schema
import tkinter as tk
from tkinter import messagebox
from tkinter import ttk # For themed widgets (optional, but often looks better)
from uuid import uuid4

class PharmacySalesApp:
    def __init__(self, master, spark):
        self.master = master
        self.spark = spark
        self.collected_data = {}
        self.cart_products = []
        self.cart_id = str(uuid4())
        self.cart_total_price_value = 0
        self.cart_total_price_value_instance = 0
        self.selected_product = None

        row_num = 0

        # Update Pricing Table
        modified_date = datetime(2000,1,1)
        medicine_pricing_table_name = "TA_PRECO_MEDICAMENTO_GOV.csv"
        try:
            modified_date = os.path.getmtime(medicine_pricing_table_name)
            modified_date = datetime.fromtimestamp(modified_date)
        except:
            pass

        today = datetime.today()
        today = datetime(today.year, today.month, today.day)
        if modified_date < today:
            download_medicine_pricing_table()
            self.df_pricing = self.spark.read.option("delimiter", ";").option("header", True).csv(medicine_pricing_table_name)
            self.df_pricing.write.format("delta").mode("overwrite").option("delta.columnMapping.mode", "name").save("/sales/medicine_pricing")

        self.df_pricing = (
            self.spark.read.format("delta").load("/sales/medicine_pricing")
            .select("SUBSTÂNCIA", "LABORATÓRIO", "EAN 1", "EAN 2", "EAN 3", "PRODUTO", "APRESENTAÇÃO", "PF Sem Impostos", "PF 12 %")
            .withColumn("substance_name", F.col("SUBSTÂNCIA"))
            .withColumn("industry_name", F.col("LABORATÓRIO"))
            .withColumn("product_name", F.concat(F.col("PRODUTO"), F.lit(" "), F.col("APRESENTAÇÃO")))
            .withColumn("unit_price", F.regexp_replace(F.col("PF Sem Impostos"), ",", ".").cast("float"))
            .withColumn("unit_price_after_taxes", F.regexp_replace(F.col("PF 12 %"), ",", ".").cast("float"))
            .withColumn("taxes_price", (F.col("unit_price_after_taxes") - F.col("unit_price")))
            .select("substance_name", "industry_name", "EAN 1", "EAN 2", "EAN 3", "product_name", "unit_price", "taxes_price", "unit_price_after_taxes")
            .filter(F.col("product_name").isNotNull())
        )
        self.df_pricing.cache()

        self.product_names = self.df_pricing.select(F.col("product_name")).sort(F.asc("product_name")).collect()
        self.product_names = [row.product_name for row in self.product_names]

        master.title("Máquina Registradora")
        master.geometry("400x600") # Adjust window size as needed
        master.resizable(True, True)
        master.state('zoomed') # NEW: Maximizes the window for Windows/Linux

        # To store the ID of the scheduled filter call for debouncing
        self._filter_after_id = None

        # Variables for in-cell editing in Treeview
        self.active_editor = None # Holds the ttk.Spinbox widget when editing
        self._editing_item = None # Holds the Treeview item ID being edited
        self._editing_column_id = None # Holds the Treeview column identifier (e.g., '#3')

        # Create a main frame to hold the scrollable area and the output area
        self.main_frame = ttk.Frame(master)
        self.main_frame.pack(fill="both", expand=True, padx=10, pady=10)

        # Control when treeview_prod is submitted or not
        self.was_submitted = False

        # Configure grid for the main frame
        self.main_frame.columnconfigure(0, weight=1)
        self.main_frame.columnconfigure(1, weight=1)
        self.main_frame.rowconfigure(0, weight=0) # Row for Cart Total Price
        self.main_frame.rowconfigure(1, weight=1) # Row for Filter
        self.main_frame.rowconfigure(2, weight=2) # Row for Product Table
        self.main_frame.rowconfigure(3, weight=0) # Row for buttons
        self.main_frame.rowconfigure(4, weight=0) # Row for output label
        self.main_frame.rowconfigure(5, weight=2) # Row for output Treeview (increased weight)
        self.main_frame.rowconfigure(6, weight=0) # Row for horizontal scrollbar of Treeview
        self.main_frame.rowconfigure(7, weight=0) # Row for Delete button
        self.main_frame.rowconfigure(8, weight=0) # Row for Save Button
        self.main_frame.rowconfigure(9, weight=0) # Row for the new footer message

        # Cart's Total Price
        self.cart_total_price_var = tk.StringVar(value="Valor Total do Carrinho: R$ 0.00")
        self.cart_total_price_label = ttk.Label(self.main_frame, textvariable=self.cart_total_price_var, font=("Arial", 14, "bold"), anchor="center")
        self.cart_total_price_label.grid(row=row_num, column=0, columnspan=2, pady=10, sticky="ew")
        row_num += 1

        self.fields = [
            ("product_name", "Filtre um produto:", False),
            ("product_table", "Tabela de Produtos:", False),
        ]

        self.entries = {}
        self.vars = {}
        #row_num = 0

        # Create input fields for each schema column
        for field_name, label_text, nullable in self.fields:

            if field_name == "product_table":
                # Frame to hold Treeview and its scrollbars
                self.tree_frame_prod = ttk.Frame(self.main_frame)
                entry = self.tree_frame_prod
                self.tree_frame_prod.grid(row=row_num, column=0, columnspan=2, padx=10, pady=5, sticky="nsew")
                self.tree_frame_prod.columnconfigure(0, weight=1)
                self.tree_frame_prod.rowconfigure(0, weight=1)

                # Define Treeview columns
                self.tree_columns_prod = ("Fábrica", "Nome do Produto", "Preço Fábrica (R$)", "Imposto (R$)", "Quantidade", "Preço Total ST (R$)")
                self.output_treeview_prod = ttk.Treeview(self.tree_frame_prod, columns=self.tree_columns_prod, show="headings")

                # Configure Treeview headings
                for col in self.tree_columns_prod:
                    self.output_treeview_prod.heading(col, text=col, anchor="center")
                    self.output_treeview_prod.column(col, width=100, anchor="center") # Default width

                # Adjust specific column widths for better display
                self.output_treeview_prod.column("Fábrica", width=120, anchor="e")
                self.output_treeview_prod.column("Nome do Produto", width=180, anchor="e")
                self.output_treeview_prod.column("Preço Fábrica (R$)", width=40, anchor="e")
                self.output_treeview_prod.column("Imposto (R$)", width=40, anchor="e")
                self.output_treeview_prod.column("Quantidade", width=30, anchor="e")
                self.output_treeview_prod.column("Preço Total ST (R$)", width=40, anchor="e")

                # Add vertical scrollbar for Treeview
                self.tree_v_scrollbar_prod = ttk.Scrollbar(self.tree_frame_prod, orient="vertical", command=self.output_treeview_prod.yview)
                self.output_treeview_prod.configure(yscrollcommand=self.tree_v_scrollbar_prod.set)
                self.tree_v_scrollbar_prod.pack(side="right", fill="y")

                # Add horizontal scrollbar for Treeview
                self.tree_h_scrollbar_prod = ttk.Scrollbar(self.tree_frame_prod, orient="horizontal", command=self.output_treeview_prod.xview)
                self.output_treeview_prod.configure(xscrollcommand=self.tree_h_scrollbar_prod.set)
                self.tree_h_scrollbar_prod.pack(side="bottom", fill="x")

                self.output_treeview_prod.pack(side="left", fill="both", expand=True)

                # Bind double click to the Treeview for cell editing
                self.output_treeview_prod.bind("<Button-1>", self._on_treeview_click)
            
            else:
                label = ttk.Label(self.main_frame, text=label_text)
                label.grid(row=row_num, column=0, sticky="w", padx=10, pady=5)

                self.vars[field_name] = tk.StringVar()
                entry = ttk.Entry(self.main_frame, textvariable=self.vars[field_name], width=50)

            if not nullable:
                label.config(text=label_text + " *") # Mark required fields

            if field_name == "product_name":
                entry.bind("<KeyRelease>", lambda event, cb=entry: self._schedule_filter_combobox_values(cb))
                # Bind <<ComboboxSelected>> to trigger the new trace method and take focus off
                entry.bind("<<ComboboxSelected>>", self._on_product_name_selected) # MODIFIED: Call new method

            # Disable total_price field as it's calculated
            if field_name not in ["product_table", "product_name", "discount_applied", "quantity"]:
                entry.config(state=tk.DISABLED)

            entry.grid(row=row_num, column=1, sticky="ew", padx=10, pady=5)
            self.entries[field_name] = entry
            row_num += 1

        # Submit Button
        self.submit_button = ttk.Button(self.main_frame, text="Registrar Produto (CTRL + ENTER)", command=self.submit_data)
        self.submit_button.grid(row=2, column=0, pady=10, padx=5, sticky="ew")

        # Clear Button
        self.clear_button = ttk.Button(self.main_frame, text="Limpar Campos", command=self.clear_fields)
        self.clear_button.grid(row=2, column=1, pady=10, padx=5, sticky="ew")

        # NEW: Delete Selected Row Button
        self.delete_button = ttk.Button(self.main_frame, text="Excluir Produto (Delete)", command=self.delete_selected_row)
        self.delete_button.grid(row=3, column=0, columnspan=2, pady=5, sticky="ew") # Shifted to new row 7

        # Output Text Area
        self.output_label = ttk.Label(self.main_frame, text="Produtos Registrados")
        self.output_label.grid(row=4, column=0, columnspan=2, sticky="w", padx=10, pady=5)

        # Frame to hold Treeview and its scrollbars
        self.tree_frame = ttk.Frame(self.main_frame)
        self.tree_frame.grid(row=5, column=0, columnspan=2, padx=10, pady=5, sticky="nsew")
        self.tree_frame.columnconfigure(0, weight=1)
        self.tree_frame.rowconfigure(0, weight=1)

        # Define Treeview columns
        self.tree_columns = ("Fábrica", "Nome do Produto", "Preço Líquido ST (R$)", "Quantidade", "Preço Total ST (R$)")
        self.output_treeview = ttk.Treeview(self.tree_frame, columns=self.tree_columns, show="headings")

        # Configure Treeview headings
        for col in self.tree_columns:
            self.output_treeview.heading(col, text=col, anchor="center")
            self.output_treeview.column(col, width=100, anchor="center") # Default width

        # Adjust specific column widths for better display
        self.output_treeview.column("Fábrica", width=180, anchor="e")
        self.output_treeview.column("Nome do Produto", width=180, anchor="e")
        self.output_treeview.column("Preço Líquido ST (R$)", width=100, anchor="e")
        self.output_treeview.column("Quantidade", width=80, anchor="e")
        self.output_treeview.column("Preço Total ST (R$)", width=120, anchor="e")

        # Add vertical scrollbar for Treeview
        self.tree_v_scrollbar = ttk.Scrollbar(self.tree_frame, orient="vertical", command=self.output_treeview.yview)
        self.output_treeview.configure(yscrollcommand=self.tree_v_scrollbar.set)
        self.tree_v_scrollbar.pack(side="right", fill="y")

        # Add horizontal scrollbar for Treeview
        self.tree_h_scrollbar = ttk.Scrollbar(self.tree_frame, orient="horizontal", command=self.output_treeview.xview)
        self.output_treeview.configure(xscrollcommand=self.tree_h_scrollbar.set)
        self.tree_h_scrollbar.pack(side="bottom", fill="x")

        self.output_treeview.pack(side="left", fill="both", expand=True)

        # NEW: Save Button
        self.save_button = ttk.Button(self.main_frame, text="Salvar (CTRL + S)", command=self.save_to_delta)
        self.save_button.grid(row=7, column=0, columnspan=2, pady=5, sticky="ew") # Shifted to new row 7

        # NEW: Footer Message Label
        self.footer_message_var = tk.StringVar(value="")
        self.footer_label = ttk.Label(self.main_frame, textvariable=self.footer_message_var, anchor="center", font=("Arial", 10, "italic"))
        self.footer_label.grid(row=8, column=0, columnspan=2, pady=5, sticky="w")

        # NEW: Bind the Enter key to the submit_data method for the entire window
        self.master.bind("<Control-Return>", lambda event=None: self.submit_data())
        self.master.bind("<Control-s>", lambda event=None: self.save_to_delta())
        self.master.bind("<Delete>", lambda event=None: self.delete_selected_row())

    def _schedule_filter_combobox_values(self, combobox):
        """Schedules the filtering of combobox values after a delay."""
        if self._filter_after_id:
            self.master.after_cancel(self._filter_after_id)
        # Schedule the actual filtering after 3000 milliseconds (3 seconds)
        self._filter_after_id = self.master.after(3000, self._perform_filter_combobox_values, combobox)

    def _perform_filter_combobox_values(self, filter_input):
        """Performs the actual filtering of combobox values."""
        current_text = filter_input.get().lower()

        # TODO: If the values were not submitted, submit values with Quantidade > 0 and then clear the fields
        # and set self.was_submitted to False
        if not self.was_submitted:
            # Submit rows with Quantidade > 0
            pass

        self._delete_all_rows_from_treeview_prod()
        self.was_submitted = False
        
        if not current_text:
            self._delete_all_rows_from_treeview_prod()
        else:
            df_filtered = self.df_pricing.where(f"lower(product_name) LIKE '%{current_text.lower()}%'")
            substance = df_filtered.select("substance_name").dropDuplicates().collect()[0]["substance_name"]
            
            df_filtered = (
                df_filtered
                .union(
                    self.df_pricing
                    .where(f"lower(substance_name) LIKE '%{substance.lower()}%' AND lower(product_name) NOT LIKE '%{current_text.lower()}%'")
                    .sort(F.asc("product_name"))
                )
                .toPandas()
            )

            for index, row in df_filtered.iterrows():
                self.output_treeview_prod.insert(
                    "", 
                    "end", 
                    values=(
                        row["industry_name"], 
                        row["product_name"], 
                        f'{row["unit_price"]:.2f}'.replace(".", ","),
                        f'{row["taxes_price"]:.2f}'.replace(".", ","),
                        0,
                        f'{row["unit_price_after_taxes"]:.2f}'.replace(".", ","),
                    )
                )
        
        # Reset the scheduled ID
        self._filter_after_id = None

    def _on_product_name_selected(self, event):
        """
        This method is called when a product name is explicitly selected from the combobox dropdown.
        You can add specific logic here that should trigger only upon selection.
        """
        self.selected_product = self.vars["product_name"].get()
        self.selected_product = self.df_pricing.filter(F.col("product_name") == F.lit(self.selected_product)).collect()[0]
        self.master.focus_set() # Keep focus on the main window after selection

        self.entries["product_id"].config(state=tk.NORMAL)
        self.vars["product_id"].set(self.selected_product["EAN 1"]) # Format to 2 decimal places
        self.entries["product_id"].config(state=tk.DISABLED) # Re-disable after update

        self.entries["industry_name"].config(state=tk.NORMAL)
        self.vars["industry_name"].set(self.selected_product["industry_name"]) # Format to 2 decimal places
        self.entries["industry_name"].config(state=tk.DISABLED) # Re-disable after update

        self.entries["unit_price"].config(state=tk.NORMAL)
        self.vars["unit_price"].set(f'{self.selected_product["unit_price"]:.2f}') # Format to 2 decimal places
        self.entries["unit_price"].config(state=tk.DISABLED) # Re-disable after update

        self.entries["taxes_price"].config(state=tk.NORMAL)
        self.vars["taxes_price"].set(f'{self.selected_product["taxes_price"]:.2f}') # Format to 2 decimal places
        self.entries["taxes_price"].config(state=tk.DISABLED) # Re-disable after update

    def _on_treeview_click(self, event): # RENAMED from _on_treeview_double_click
        """Handles single-click event on the Treeview for cell editing."""
        # If an editor is already active, end its editing first
        if self.active_editor:
            self._end_cell_edit(None)

        region = self.output_treeview_prod.identify_region(event.x, event.y)
        if region != "cell":
            return

        column_id = self.output_treeview_prod.identify_column(event.x)
        item_id = self.output_treeview_prod.identify_row(event.y)

        column_name = self.output_treeview_prod.heading(column_id, 'text')
        if column_name == "Quantidade":
            x, y, width, height = self.output_treeview_prod.bbox(item_id, column_id)

            quantity_index_in_values = self.tree_columns_prod.index("Quantidade")
            current_quantity = self.output_treeview_prod.item(item_id, 'values')[quantity_index_in_values]

            self.active_editor = ttk.Spinbox(self.output_treeview_prod,
                                             from_=0, to=9999,
                                             textvariable=tk.StringVar(value=current_quantity),
                                             wrap=False)

            self.active_editor.place(x=x, y=y, width=width, height=height)
            self.active_editor.focus_set()

            self._editing_item = item_id
            self._editing_column_id = column_id

            self.active_editor.bind("<Return>", self._end_cell_edit)
            self.active_editor.bind("<FocusOut>", self._end_cell_edit)

        else:
            if self.active_editor:
                self._end_cell_edit(None)

    def _end_cell_edit(self, event):
        """Ends cell editing, validates input, updates Treeview, and recalculates total price."""
        if not self.active_editor:
            return

        if event and event.type == "9" and str(event.widget) == str(self.active_editor):
            if self.active_editor.winfo_containing(event.x_root, event.y_root) == self.active_editor:
                return

        item = self._editing_item
        column_id = self._editing_column_id

        try:
            new_quantity_str = self.active_editor.get()
            new_quantity = int(new_quantity_str) if new_quantity_str != "" else 0

            if new_quantity < 0:
                raise ValueError("Quantity cannot be negative.")

            current_values = list(self.output_treeview_prod.item(item, 'values'))

            quantity_column_index = self.tree_columns_prod.index("Quantidade")
            current_values[quantity_column_index] = new_quantity

            total_price_column_index = self.tree_columns_prod.index("Preço Total ST (R$)")

            total_price_str = current_values[total_price_column_index]
            total_price = float(total_price_str.replace(',', '.'))

            recalculated_total = new_quantity * total_price
            recalculated_total = recalculated_total if recalculated_total > 0 else total_price

            current_values[total_price_column_index] = f"{recalculated_total:.2f}".replace('.', ',')

            self.output_treeview_prod.item(item, values=current_values)

        except ValueError as e:
            messagebox.showerror("Invalid Input", f"Invalid quantity: {e}. Please enter a whole number.")

        except Exception as e:
            messagebox.showerror("Error", f"An unexpected error occurred: {e}")

        finally:
            if self.active_editor:
                self.active_editor.destroy()
            self.active_editor = None
            self._editing_item = None
            self._editing_column_id = None

    def on_frame_configure(self, event):
        """Update the scrollregion of the canvas when the inner frame changes size."""
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def on_canvas_configure(self, event):
        """Update the canvas window size when the canvas itself changes size."""
        self.canvas.itemconfig(self.canvas_window, width=self.canvas.winfo_width())

    def _on_mousewheel(self, event):
        """Handle mouse wheel scrolling."""
        if event.num == 5 or event.delta == -120:  # Scroll down
            self.canvas.yview_scroll(1, "unit")
        elif event.num == 4 or event.delta == 120:  # Scroll up
            self.canvas.yview_scroll(-1, "unit")

    def clear_fields(self):
        """Clears all input fields in the UI."""
        for field_name, _, _ in self.fields:
            if field_name == "is_prescription_item":
                self.vars[field_name].set(False)
            else:
                self.vars[field_name].set("")

        # Set focus to the first input field (transaction_id)
        self.entries["product_id"].focus_set()

    def _delete_all_rows_from_treeview(self):
        for item in self.output_treeview.get_children():
            self.output_treeview.delete(item)

    def _delete_all_rows_from_treeview_prod(self):
        for item in self.output_treeview_prod.get_children():
            self.output_treeview_prod.delete(item)

    def delete_selected_row(self):
        """Deletes the selected row(s) from the Treeview."""
        selected_items = self.output_treeview.selection()
        if not selected_items:
            messagebox.showwarning("Nenhuma linha selecionada", "Por favor selecione uma linha para excluir.")
            #self.update_output("Nenhuma linha selecionada para excluir.")
            return

        confirm = messagebox.askyesno("Confirmar Exclusão", "Tem certeza que quer excluir a linha selecionada?")
        if confirm:
            for item in selected_items:
                # Deleting from self.cart_products
                index = self.output_treeview.index(item)
                self.cart_products.pop(index)

                item_total_price_after_taxes = float(self.output_treeview.item(item, 'values')[4])
                self.cart_total_price_value -= item_total_price_after_taxes
                self.cart_total_price_value_instance -= item_total_price_after_taxes
                
                self.output_treeview.delete(item)
                self.cart_total_price_var.set(f"Valor Total do Carrinho: {self.cart_total_price_value_instance:.2f}")


    def submit_data(self):
        """Collects data from the UI fields, validates, and displays it."""

        self.collected_data = {}

        for item in self.output_treeview_prod.get_children():
            values = self.output_treeview_prod.item(item, 'values')

            self.collected_data["cart_id"] = self.cart_id
            self.collected_data["transaction_id"] = str(uuid4())
            self.collected_data["sale_datetime"] = datetime.now()

            industry_name_index_in_values = self.tree_columns_prod.index("Fábrica")
            product_name_index_in_values = self.tree_columns_prod.index("Nome do Produto")
            unit_price_index_in_values = self.tree_columns_prod.index("Preço Fábrica (R$)")
            taxes_price_index_in_values = self.tree_columns_prod.index("Imposto (R$)")
            quantity_index_in_values = self.tree_columns_prod.index("Quantidade")
            total_price_after_taxes_index_in_values = self.tree_columns_prod.index("Preço Total ST (R$)")

            if int(values[quantity_index_in_values]) > 0:

                self.collected_data["sale_datetime"] = datetime.now()
                self.collected_data["industry_name"] = values[industry_name_index_in_values].strip()
                self.collected_data["product_name"] = values[product_name_index_in_values].strip()
                self.collected_data["unit_price"] = values[unit_price_index_in_values].strip()
                self.collected_data["taxes_price"] = values[taxes_price_index_in_values].strip()

                unit_price = float(self.collected_data["unit_price"].replace(",", "."))
                taxes_price = float(self.collected_data["taxes_price"].replace(",", "."))
                liquid_price_after_taxes = unit_price - taxes_price

                self.collected_data["liquid_price_after_taxes"] = f"{liquid_price_after_taxes:.2f}".replace(".", ",")
                self.collected_data["quantity"] = values[quantity_index_in_values].strip()
                self.collected_data["total_price_after_taxes"] = values[total_price_after_taxes_index_in_values].strip()
                
                total_price_after_taxes = float(self.collected_data["total_price_after_taxes"].replace(",", "."))
                
                #self.output_treeview.insert(values)
                self.cart_total_price_value_instance += total_price_after_taxes

                # Display collected data (you would typically send this to Spark/Delta Lake)
                #output_str = "Data Collected Successfully:\n"
                industry_name = self.collected_data.get("industry_name", "")
                product_name = self.collected_data.get("product_name", "")
                liquid_price_after_taxes = f"{self.collected_data.get('liquid_price_after_taxes', '0,00')}"
                quantity = self.collected_data.get("quantity", 0)
                total_price_after_taxes = f"{self.collected_data.get('total_price_after_taxes', '0,00')}"

                self.output_treeview.insert("", "end", values=(industry_name, product_name, liquid_price_after_taxes, quantity, total_price_after_taxes))
                
                self.cart_products.append(self.collected_data.copy())

                # Update the Header permanently
                self.cart_total_price_value = self.cart_total_price_value_instance
                self.cart_total_price_var.set(f"Valor Total do Carrinho: {self.cart_total_price_value_instance:.2f}".replace(".", ","))

        print(self.cart_products)

        # Clearing the fields
        #self.clear_fields()
        self._delete_all_rows_from_treeview_prod()
        #messagebox.showinfo("Success", "Data collected and validated successfully!")

        # Set focus to the first input field (transaction_id)
        self.vars["product_name"].set("")
        self.entries["product_name"].focus_set()

    def save_to_delta(self):
        if len(self.cart_products) < 1:
            messagebox.showwarning("Carrinho vazio", "Nenhum produto registrado para salvar. Nada será salvo.")
        else:
            confirm = messagebox.askyesno("Confirmar salvamento", "Tem certeza que deseja salvar e limpar o carrinho?")
            if confirm:

                self.footer_message_var.set("Salvando o carrinho, por favor aguarde...")

                print(self.cart_products)
                df = self.spark.createDataFrame(self.cart_products, schema=pharmacy_sales_schema)
                df.write.format("delta").mode("append").save("/sales/daily_sales")
                
                self.cart_products = []
                self.cart_total_price_value = 0
                self.cart_total_price_value_instance = 0

                self._delete_all_rows_from_treeview()

                self.cart_total_price_var.set(f"Valor Total do Carrinho: 0.00")

                self.footer_message_var.set("")

                messagebox.showinfo("Carrinho salvo", "Carrinho salvo com sucesso! A maquina registradora será limpa.")

            

# Main part of the script
if __name__ == "__main__":

    builder = (
        pyspark.sql.SparkSession.builder
        .appName("RegisterMachine")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()    

    root = tk.Tk()
    app = PharmacySalesApp(root, spark)
    root.mainloop()

    spark.stop()
