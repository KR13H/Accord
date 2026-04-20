import React, { useEffect, useState } from "react";

const InventoryManager = ({ syncEvent }) => {
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showAutoGenOnly, setShowAutoGenOnly] = useState(false);
  const [newItem, setNewItem] = useState({
    item_name: "",
    factory_serial: "",
    current_stock: 0,
    minimum_stock_level: 0,
    unit_price: 0,
  });

  // Fetch inventory items
  useEffect(() => {
    fetchItems();
  }, []);

  useEffect(() => {
    if (!syncEvent || syncEvent.event !== "INVENTORY_UPDATED") {
      return;
    }
    fetchItems();
  }, [syncEvent]);

  const fetchItems = async () => {
    try {
      setLoading(true);
      const response = await fetch("/api/v1/sme/inventory/items");
      const data = await response.json();
      if (data.status === "ok") {
        setItems(data.items);
      } else {
        setError("Failed to fetch inventory items");
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleAddItem = async (e) => {
    e.preventDefault();
    try {
      const response = await fetch("/api/v1/sme/inventory/items", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          item_name: newItem.item_name,
          factory_serial: newItem.factory_serial || null,
          current_stock: parseFloat(newItem.current_stock),
          minimum_stock_level: parseFloat(newItem.minimum_stock_level),
          unit_price: parseFloat(newItem.unit_price),
        }),
      });
      const data = await response.json();
      if (data.status === "ok") {
        setItems([...items, data.item]);
        setNewItem({
          item_name: "",
          factory_serial: "",
          current_stock: 0,
          minimum_stock_level: 0,
          unit_price: 0,
        });
      } else {
        alert("Failed to add item");
      }
    } catch (err) {
      alert(`Error: ${err.message}`);
    }
  };

  // Filter items based on toggle
  const filteredItems = showAutoGenOnly
    ? items.filter((item) => item.is_system_generated)
    : items;

  if (loading) {
    return (
      <div className="flex items-center justify-center p-8">
        <div className="text-gray-500">Loading inventory...</div>
      </div>
    );
  }

  return (
    <div className="w-full max-w-6xl mx-auto p-6 bg-white rounded-lg shadow-lg">
      <h1 className="text-3xl font-bold text-gray-800 mb-6">
        Inventory Management
      </h1>

      {error && (
        <div className="mb-4 p-4 bg-red-100 border border-red-400 text-red-700 rounded">
          {error}
        </div>
      )}

      {/* Add New Item Form */}
      <div className="mb-8 p-6 bg-blue-50 rounded-lg border border-blue-200">
        <h2 className="text-xl font-semibold text-gray-700 mb-4">
          Add New Item
        </h2>
        <form onSubmit={handleAddItem} className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <input
            type="text"
            placeholder="Item Name *"
            required
            value={newItem.item_name}
            onChange={(e) =>
              setNewItem({ ...newItem, item_name: e.target.value })
            }
            className="px-3 py-2 border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
          <input
            type="text"
            placeholder="Factory Serial (optional)"
            value={newItem.factory_serial}
            onChange={(e) =>
              setNewItem({ ...newItem, factory_serial: e.target.value })
            }
            className="px-3 py-2 border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
          <input
            type="number"
            placeholder="Current Stock"
            min="0"
            step="0.01"
            value={newItem.current_stock}
            onChange={(e) =>
              setNewItem({ ...newItem, current_stock: e.target.value })
            }
            className="px-3 py-2 border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
          <input
            type="number"
            placeholder="Minimum Stock Level"
            min="0"
            step="0.01"
            value={newItem.minimum_stock_level}
            onChange={(e) =>
              setNewItem({ ...newItem, minimum_stock_level: e.target.value })
            }
            className="px-3 py-2 border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
          <input
            type="number"
            placeholder="Unit Price"
            min="0"
            step="0.01"
            value={newItem.unit_price}
            onChange={(e) =>
              setNewItem({ ...newItem, unit_price: e.target.value })
            }
            className="px-3 py-2 border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
          <button
            type="submit"
            className="px-6 py-2 bg-blue-600 text-white font-semibold rounded hover:bg-blue-700 transition"
          >
            Add Item
          </button>
        </form>
      </div>

      {/* Filter Toggle */}
      <div className="mb-6 flex items-center gap-4">
        <label className="flex items-center gap-2 cursor-pointer">
          <input
            type="checkbox"
            checked={showAutoGenOnly}
            onChange={(e) => setShowAutoGenOnly(e.target.checked)}
            className="w-4 h-4 text-blue-600 rounded focus:ring-2 focus:ring-blue-500"
          />
          <span className="text-sm font-medium text-gray-700">
            Show Auto-Generated Serials Only
          </span>
        </label>
        <span className="text-xs text-gray-500">
          ({filteredItems.length} of {items.length} items)
        </span>
      </div>

      {/* Inventory Table */}
      <div className="overflow-x-auto">
        <table className="w-full border-collapse">
          <thead>
            <tr className="bg-gray-100 border-b-2 border-gray-300">
              <th className="px-4 py-3 text-left font-semibold text-gray-700">
                Item Name
              </th>
              <th className="px-4 py-3 text-left font-semibold text-gray-700">
                Serial Number
              </th>
              <th className="px-4 py-3 text-center font-semibold text-gray-700">
                Current Stock
              </th>
              <th className="px-4 py-3 text-center font-semibold text-gray-700">
                Min Level
              </th>
              <th className="px-4 py-3 text-right font-semibold text-gray-700">
                Unit Price
              </th>
            </tr>
          </thead>
          <tbody>
            {filteredItems.length === 0 ? (
              <tr>
                <td
                  colSpan="5"
                  className="px-4 py-8 text-center text-gray-500"
                >
                  {items.length === 0
                    ? "No inventory items yet. Add one above!"
                    : "No auto-generated items to show."}
                </td>
              </tr>
            ) : (
              filteredItems.map((item) => (
                <tr
                  key={item.id}
                  className="border-b border-gray-200 hover:bg-gray-50 transition"
                >
                  <td className="px-4 py-3 font-medium text-gray-900">
                    {item.item_name}
                  </td>
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-2">
                      <code className="text-sm font-mono bg-gray-100 px-2 py-1 rounded">
                        {item.system_serial}
                      </code>
                      {item.is_system_generated && (
                        <span className="inline-block px-2 py-1 text-xs font-semibold text-yellow-800 bg-yellow-200 rounded">
                          Auto-Gen
                        </span>
                      )}
                    </div>
                  </td>
                  <td className="px-4 py-3 text-center text-gray-700">
                    {parseFloat(item.current_stock).toFixed(2)}
                  </td>
                  <td className="px-4 py-3 text-center text-gray-700">
                    {parseFloat(item.minimum_stock_level).toFixed(2)}
                  </td>
                  <td className="px-4 py-3 text-right text-gray-700">
                    ₹{parseFloat(item.unit_price).toFixed(2)}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Summary Stats */}
      {items.length > 0 && (
        <div className="mt-6 grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="p-4 bg-blue-50 rounded border border-blue-200">
            <div className="text-sm text-gray-600">Total Items</div>
            <div className="text-2xl font-bold text-blue-600">{items.length}</div>
          </div>
          <div className="p-4 bg-yellow-50 rounded border border-yellow-200">
            <div className="text-sm text-gray-600">Auto-Generated Serials</div>
            <div className="text-2xl font-bold text-yellow-600">
              {items.filter((i) => i.is_system_generated).length}
            </div>
          </div>
          <div className="p-4 bg-purple-50 rounded border border-purple-200">
            <div className="text-sm text-gray-600">Factory Serials</div>
            <div className="text-2xl font-bold text-purple-600">
              {items.filter((i) => !i.is_system_generated).length}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default InventoryManager;
