
open ExtLib
open Printf

let log = Log.from "lwt_gauge"

type stream_type =
  | From
  | FromDirect
  | Bounded
  | Unbounded
  | UnboundedWithRef
  | OfList
  | OfArray
  | OfString
  | Clone

let string_of_stream_type = function
  | From -> "from"
  | FromDirect -> "from_direct"
  | Bounded -> "create_bounded"
  | Unbounded -> "create"
  | UnboundedWithRef -> "create_with_reference"
  | OfList -> "of_list"
  | OfArray -> "of_array"
  | OfString -> "of_string"
  | Clone -> "clone"

type stream = {
  type_ : stream_type;
  name : string option;
  is_closed : unit -> bool;
  is_empty : unit -> bool;
}

type gauge = {
  mutable streams : stream Dllist.node_t option;
}

let gauge g type_ ~name stream =
  let is_closed () = Lwt_stream.is_closed stream in
  let is_empty () =
    match Lwt.state (Lwt_stream.is_empty stream) with
    | Return is_empty -> is_empty
    | Sleep | Fail _ -> assert false (* if is_closed, is_empty is guaranteed not to block *)
  in
  let s = { type_; name; is_closed; is_empty; } in
  let handle =
    match g.streams with
    | Some streams -> Dllist.prepend streams s
    | None ->
    let streams = Dllist.create s in
    g.streams <- Some streams;
    streams
  in
  let remove () =
    match g.streams with
    | None -> assert false
    | Some streams when streams != handle -> Dllist.remove handle
    | Some _ ->
    let next = Dllist.drop handle in
    g.streams <- if next != handle then Some next else None
  in
  let fin = Lwt_stream.from_direct (fun () -> remove (); None) in
  Lwt_stream.append stream fin

let end_gauge { streams; } =
  match streams with
  | None -> ()
  | Some streams ->
  let streams = Dllist.to_list streams in
  log #warn "%d streams are still active:" (List.length streams);
  List.iteri begin fun i { name; type_; is_closed; is_empty; } ->
    let attrs =
      match is_closed () with
      | false -> []
      | true ->
      "closed" ::
      match is_empty () with
      | false -> []
      | true ->
      "empty" :: []
    in
    log #warn "%4d) Lwt_stream.%s ~name:%S%s"
      (i + 1) (string_of_stream_type type_) (Option.default "<unnamed>" name)
      (match attrs with [] -> "" | _ -> sprintf " (* %s *)" (String.concat ", " attrs))
  end streams

module Lwt_stream = struct

  include Lwt_stream

  let tls = Lwt.new_key ()

  let from ?name f =
    match Lwt.get tls with
    | None -> from f
    | Some g -> gauge g From ~name (from f)

  let from_direct ?name f =
    match Lwt.get tls with
    | None -> from_direct f
    | Some g -> gauge g FromDirect ~name (from_direct f)

  let create ?name () =
    match Lwt.get tls with
    | None -> create ()
    | Some g ->
    let (s, f) = create () in
    gauge g Unbounded ~name s, f

  let create_with_reference ?name () =
    match Lwt.get tls with
    | None -> create_with_reference ()
    | Some g ->
    let (s, f, r) = create_with_reference () in
    gauge g Unbounded ~name s, f, r

  let create_bounded ?name n =
    match Lwt.get tls with
    | None -> create_bounded n
    | Some g ->
    let (s, p) = create_bounded n in
    gauge g Bounded ~name s, p

  let of_list ?name l =
    match Lwt.get tls with
    | None -> of_list l
    | Some g -> gauge g OfList ~name (of_list l)

  let of_array ?name a =
    match Lwt.get tls with
    | None -> of_array a
    | Some g -> gauge g OfArray ~name (of_array a)

  let of_string ?name s =
    match Lwt.get tls with
    | None -> of_string s
    | Some g -> gauge g OfString ~name (of_string s)

  let clone ?name s =
    match Lwt.get tls with
    | None -> clone s
    | Some g -> gauge g Clone ~name (clone s)

  let with_gauge f =
    let gauge = { streams = None; } in
    Lwt.with_value tls (Some gauge) @@ fun () ->
    (f ()) [%lwt.finally end_gauge gauge; Lwt.return_unit; ]

end
