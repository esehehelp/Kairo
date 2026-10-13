package envspec

import "testing"

func TestValidate(t *testing.T) {
	for _, env := range []map[string]string{
		{"": "x"}, {" A": "x"}, {"A ": "x"}, {"A=B": "x"}, {"A\x00": "x"}, {"A": "x\x00y"},
		{"KAIRO_TOKEN": "x"}, {"Kairo_Api_Url": "x"}, {"WSLENV": "x"}, {"wslenv": "x"},
	} {
		if err := Validate(env, false); err == nil {
			t.Fatalf("accepted %q", env)
		}
	}
	ok := map[string]string{"PYTHONUTF8": "1", "Path": `C:\tools;C:\Windows`, "EMPTY": "", "KAIRO": "not a Kairo variable", "CUDA_VISIBLE_DEVICES": "-1"}
	if err := Validate(ok, false); err != nil {
		t.Fatal(err)
	}
	if err := Validate(map[string]string{"cuda_visible_devices": "0"}, true); err == nil {
		t.Fatal("accepted CUDA_VISIBLE_DEVICES with a GPU lease")
	}
}

func TestMerge(t *testing.T) {
	if Merge(nil, map[string]string{}) != nil {
		t.Fatal("empty merge is not nil")
	}
	base := map[string]string{"A": "1", "B": "2"}
	merged := Merge(base, map[string]string{"B": "3", "C": "4"})
	if len(merged) != 3 || merged["A"] != "1" || merged["B"] != "3" || merged["C"] != "4" || base["B"] != "2" {
		t.Fatalf("merged %v base %v", merged, base)
	}
}
