
open ExtLib
open Printf

let log = Log.from "lwt_gauge"

module Gauge = struct

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

  type probe = {
    type_ : stream_type;
    name : string option;
    count : int;
    size : int option;
    get : int;
    push : int;
    is_closed : bool;
    is_empty : bool;
  }

  type props = {
    type_ : stream_type;
    name : string option;
    mutable count : int;
    mutable size : int option;
    mutable get : int;
    mutable push : int;
  }

  type controls = <
    incr : unit;
    decr : unit;
    resize : int -> unit;
    incr_get : unit;
    decr_get : unit;
    incr_push : unit;
    decr_push : unit;
    props : props;
  >

  type gauge = <
    is_closed : bool;
    is_empty : bool;
    controls : controls;
  >

  let is_active : unit Lwt.key = Lwt.new_key ()

  type any_lwt_stream = Lwt_stream : 'a Lwt_stream.t -> any_lwt_stream

  module StreamHashtbl = Ephemeron.K1.Make(struct
    type t = any_lwt_stream
    let equal = (==)
    let hash = Hashtbl.hash
  end)

  type global_state = {
    mutable gauges : gauge Dllist.node_t option;
    streams : gauge StreamHashtbl.t;
  }

  let global_state = {
    gauges = None;
    streams = StreamHashtbl.create 10;
  }

  let make'' type_ ~name ?(count=0) ?size () =
    let props = { type_; name; count; size; get = 0; push = 0; } in
    object
      method incr = props.count <- props.count + 1
      method decr = props.count <- props.count - 1
      method resize n = props.size <- Some n
      method incr_get = props.get <- props.get + 1
      method decr_get = props.get <- props.get - 1
      method incr_push = props.push <- props.push + 1
      method decr_push = props.push <- props.push - 1
      method props = props
    end

  let make' controls stream =
    object
      method is_closed = Lwt_stream.is_closed stream
      method is_empty =
        match Lwt.state (Lwt_stream.is_empty stream) with
        | Return is_empty -> is_empty
        | Sleep | Fail _ -> assert false (* if is_closed, is_empty is guaranteed not to block *)
      method controls = controls
    end

  let make type_ ~name ?count ?size stream =
    let controls = make'' type_ ~name ?count ?size () in
    make' controls stream

  let attach s stream =
    let handle =
      match global_state.gauges with
      | Some gauges -> Dllist.prepend gauges s
      | None ->
      let gauges = Dllist.create s in
      global_state.gauges <- Some gauges;
      gauges
    in
    let remove () =
      match global_state.gauges with
      | None -> assert false
      | Some gauges when gauges != handle -> Dllist.remove handle
      | Some _ ->
      let next = Dllist.drop handle in
      global_state.gauges <- if next != handle then Some next else None
    in
    let get () =
      begin
        s #controls #incr_get;
        Lwt_stream.get stream
      end [%finally
        s #controls #decr_get;
        Lwt.return_unit;
      ]
    in
    Lwt_stream.from begin fun () ->
      match%lwt get () with
      | Some _ as x -> s #controls #decr; Lwt.return x
      | None -> remove (); Lwt.return_none
    end

  let gauge type_ ~name ?count ?size stream =
    let s = make type_ ~name ?count ?size stream in
    attach s stream

  let probe_all () =
    match Lwt.get is_active with
    | None -> []
    | Some _ ->
    match global_state.gauges with
    | None -> []
    | Some gauges ->
    Dllist.to_list gauges |>
    List.map begin fun s ->
      let { type_; name; count; size; get; push; peeked; } = s #controls #props in
      let is_closed = s #is_closed in
      let is_empty = is_closed && s #is_empty in
      { type_; name; count; size; get; push; peeked; is_closed; is_empty; }
    end

  let show_probe { type_; name; count; size; get; push; peeked; is_closed; is_empty; } =
    let attrs = match is_closed with false -> [] | true -> "closed" :: [] in
    let attrs = match is_empty with false -> attrs | true -> "empty" :: attrs in
    let attrs = match get with 0 -> attrs | _ -> sprintf "%d reading" get :: attrs in
    let attrs = match push with 0 -> attrs | _ -> sprintf "%d writing" push :: attrs in
    let attrs = match peeked with false -> attrs | true -> "peeked" :: attrs in
    let attrs =
      match size with
      | Some size -> sprintf "%d/%d" count size :: attrs
      | None -> string_of_int count :: attrs
    in
    sprintf "Lwt_stream.%s ~name:%S (* %s *)"
      (string_of_stream_type type_) (Option.default "<unnamed>" name) (String.concat ", " attrs)

end

module Lwt_stream = struct

  include Lwt_stream

  open Gauge

  let from ?name f =
    match Lwt.get is_active with
    | None -> from f
    | Some _ ->
    let controls = make'' From ~name () in
    let f () =
      match%lwt f () with
      | Some _ as x -> controls #incr; Lwt.return x
      | None -> Lwt.return_none
    in
    let s = from f in
    let s' = make' controls s in
    attach s' s

  let from_direct ?name f =
    match Lwt.get is_active with
    | None -> from_direct f
    | Some _ ->
    let controls = make'' FromDirect ~name () in
    let f () =
      match f () with
      | Some _ as x -> controls #incr; x
      | None -> None
    in
    let s = from_direct f in
    let s' = make' controls s in
    attach s' s

  let create ?name () =
    match Lwt.get is_active with
    | None -> create ()
    | Some _ ->
    let (s, f) = create () in
    let s' = make Unbounded ~name s in
    let f = function Some _ as x -> s' #controls #incr; f x | None -> f None in
    attach s' s, f

  let create_with_reference ?name () =
    match Lwt.get is_active with
    | None -> create_with_reference ()
    | Some _ ->
    let (s, f, r) = create_with_reference () in
    let s' = make Unbounded ~name s in
    let f = function Some _ as x -> s' #controls #incr; f x | None -> f None in
    attach s' s, f, r

  let create_bounded ?name n =
    match Lwt.get is_active with
    | None -> create_bounded n
    | Some _ ->
    let (s, p) = create_bounded n in
    let s' = make Bounded ~name ~size:n s in
    let s = attach s' s in
    let c = s' #controls in
    s, object
      method size = p #size
      method resize n = c #resize n; p #resize n
      method push x =
        let%lwt () =
          begin
            c #incr_push;
            p #push x
          end [%finally
            c #decr_push;
            Lwt.return_unit
          ]
        in
        c #incr;
        Lwt.return_unit
      method close = p #close
      method count = p #count
      method blocked = p #blocked
      method closed = p #closed
      method set_reference : 'a. 'a -> unit = fun x -> p #set_reference x
    end

  let of_list ?name l =
    match Lwt.get is_active with
    | None -> of_list l
    | Some _ ->
    let n = List.length l in
    gauge OfList ~name ~count:n ~size:n (of_list l)

  let of_array ?name a =
    match Lwt.get is_active with
    | None -> of_array a
    | Some _ ->
    let n = Array.length a in
    gauge OfArray ~name ~count:n ~size:n (of_array a)

  let of_string ?name s =
    match Lwt.get is_active with
    | None -> of_string s
    | Some _ ->
    let n = String.length s in
    gauge OfString ~name ~count:n ~size:n (of_string s)

  let clone ?name s =
    match Lwt.get is_active with
    | None -> clone s
    | Some _ -> gauge Clone ~name (clone s)

  let peek s =
    (match find_gauge s with Some s -> s #controls #peek | None -> ());
    peek s

  let with_gauge f =
    Lwt.with_value is_active (Some ()) @@ fun () ->
    let fin () =
      match probe_all () with
      | [] -> ()
      | gauges ->
      log #warn "%d streams are still active:" (List.length gauges);
      List.iteri begin fun i probe ->
        log #warn "%4d) %s" (i + 1) (show_probe probe)
      end gauges
    in
    (f ()) [%lwt.finally fin (); Lwt.return_unit; ]

end
